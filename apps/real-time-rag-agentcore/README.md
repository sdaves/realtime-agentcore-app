# ⚡ Real-Time RAG Pipeline on AWS — AgentCore + Glue Streaming

> Framework: Strands Agents + Amazon Bedrock AgentCore + AWS Glue Streaming

A complete, working real-time RAG pipeline that streams events into a vector store and queries them through an AgentCore agent. Clone it, deploy it, break it, extend it.

**Demo cost: ~$5 for a 4-hour session.** Remember to clean up when done — OpenSearch Serverless charges ~$175/mo if left running.

---

## Architecture

```
Event Sources (IoT, API, CDC, S3)
        │
        ▼
┌───────────────────────────────┐
│  Amazon Kinesis Data Streams  │  ← On-demand, ordered, replayable
└──────────────┬────────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│  AWS Glue Streaming (PySpark)                │
│  ┌──────────┐ ┌───────────┐ ┌─────────────┐ │
│  │Parse JSON│→│Deduplicate│→│Embed (Titan)│ │  ← Parallel ThreadPoolExecutor
│  └──────────┘ └───────────┘ └─────────────┘ │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌───────────────────────────────┐
│  OpenSearch Serverless        │  ← k-NN HNSW index, conditional upserts
└──────────────┬────────────────┘
               │  MCP tool
               ▼
┌──────────────────────────────────────────────┐
│  AgentCore Gateway                           │
│  ┌────────────────┐ ┌────────┐ ┌──────────┐ │
│  │Vector search   │ │Policy  │ │Identity  │ │
│  │tool (Lambda)   │ │(Cedar) │ │(OAuth)   │ │
│  └────────────────┘ └────────┘ └──────────┘ │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌───────────────────────────────┐
│  AgentCore Runtime            │  ← Claude Haiku + Strands SDK
│  (serverless, session-isolated)│
└──────────────┬────────────────┘
               │
               ▼
         User / Application
```

---

## Prerequisites

### AWS Account & Credentials

- **AWS account** — a fresh sandbox is recommended
- **AWS CLI v2** installed and configured:
  ```bash
  # Install (Mac)
  brew install awscli

  # Configure credentials
  aws configure
  # Enter: Access Key ID, Secret Access Key, region (us-east-1), output (json)

  # Verify
  aws sts get-caller-identity
  ```
- **Bedrock model access** — enable in the AWS console:
  - Go to: AWS Console → Bedrock → Model access → Enable specific models
  - Enable: `amazon.titan-embed-text-v2:0` and `anthropic.claude-3-haiku`

### Tools

- **Node.js 18+** — required for the CDK CLI:
  ```bash
  brew install node
  ```
- **AWS CDK v2 CLI** — installed globally via npm:
  ```bash
  npm install -g aws-cdk
  cdk --version
  ```
- **Python 3.11+**:
  ```bash
  python3 --version
  ```

> New AWS accounts get up to $200 in AgentCore free tier credits.

---

## Project Structure

```
├── cdk.json                          # CDK app config
├── cdk_stack.py                      # CDK infrastructure
├── requirements.txt                  # Python dependencies
├── agent/
│   ├── agent.py                      # Strands agent for AgentCore Runtime
│   └── tool_schema.json              # Vector search tool schema
├── demo.py                           # End-to-end pipeline test script
├── glue_scripts/
│   └── streaming_job.py              # Glue PySpark job
├── lambda/
│   └── vector_search/
│       └── index.py                  # Vector search Lambda
└── scripts/
    └── generate_events.py            # Test event generator
```

---

## Step-by-Step Deployment

### Step 1: Clone and install dependencies

```bash
git clone https://github.com/sdaves/llms.git
cd llms/apps/real-time-rag-agentcore

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

This installs CDK libraries, boto3, the AgentCore SDK, the AgentCore starter toolkit CLI (`agentcore`), and the Strands Agents framework.

Verify the AgentCore CLI installed:
```bash
agentcore --help
```

### Step 2: Deploy infrastructure with CDK

```bash
cdk bootstrap    # first time only
cdk deploy RealTimeRagAgentCoreStack
```

This deploys: Kinesis stream, Glue Streaming job, OpenSearch Serverless collection, vector search Lambda, CloudWatch dashboard + alarms.

Deployment takes ~5 minutes. Note the outputs — you'll need the Lambda ARN and OpenSearch endpoint.

### Step 3: Start the Glue streaming job

```bash
aws glue start-job-run --job-name realtime-rag-streaming-job
```

The job runs continuously, processing micro-batches from Kinesis every 10 seconds.

### Step 4: Push test events and verify the pipeline

```bash
# Push 20 test events
python scripts/generate_events.py --count 20 --stream realtime-rag-stream

# Wait ~20 seconds for Glue to process, then run the demo
python demo.py
```

The demo script pushes an event, waits, queries the Lambda, pushes an update to the same entity, and queries again — proving the vector store updates in real time.

You can also test the Lambda directly:
```bash
aws lambda invoke \
  --function-name rag-vector-search \
  --payload '{"query": "order status shipped", "top_k": 3}' \
  /dev/stdout
```

### Step 5: Create the AgentCore Gateway

The `agentcore` CLI (from the starter toolkit) handles Gateway and Runtime setup.

```bash
# Create a Gateway
agentcore gateway create-mcp-gateway \
  --name RealTimeRAGGateway \
  --region us-east-1
```

Note the **Gateway ARN** and **Gateway URL** from the output.

Now add the vector search Lambda as a target. First create the IAM role:

```bash
# Create IAM role for Gateway → Lambda access
aws iam create-role \
  --role-name AgentCoreGatewayRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "bedrock-agentcore.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

aws iam put-role-policy \
  --role-name AgentCoreGatewayRole \
  --policy-name InvokeLambda \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:us-east-1:YOUR_ACCOUNT_ID:function:rag-vector-search"
    }]
  }'

# Wait for IAM propagation
sleep 10
```

Then add the Lambda as a Gateway target:

```bash
agentcore gateway create-mcp-gateway-target \
  --gateway-arn YOUR_GATEWAY_ARN \
  --gateway-url YOUR_GATEWAY_URL \
  --role-arn arn:aws:iam::YOUR_ACCOUNT_ID:role/AgentCoreGatewayRole \
  --name VectorSearchTarget \
  --target-type lambda \
  --region us-east-1
```

### Step 6: Deploy the agent to AgentCore Runtime

```bash
# Configure the agent (creates .bedrock_agentcore.yaml)
agentcore configure -e agent/agent.py --disable-memory

# Deploy to AgentCore Runtime
agentcore deploy
```

The `agentcore deploy` command packages your code, builds it via CodeBuild, creates the Runtime, and gives you the agent ARN.

### Step 7: Test the agent

```bash
# Push a fresh event
python scripts/generate_events.py \
  --type order_update \
  --entity-id ORDER-12345 \
  --status shipped \
  --count 1

# Wait ~20 seconds for the pipeline

# Invoke the agent
agentcore invoke '{"prompt": "What is the current status of order 12345?"}'
```

The agent should respond with the current status and a timestamp showing how recently the data was indexed.

Push an update and query again:
```bash
python scripts/generate_events.py \
  --type order_update \
  --entity-id ORDER-12345 \
  --status out_for_delivery \
  --count 1

# Wait ~20 seconds

agentcore invoke '{"prompt": "What is the current status of order 12345?"}'
```

The response should now show "out for delivery" — proving the real-time loop works end to end.

---

## Key Design Decisions

**Conditional upserts** — OpenSearch upserts use a Painless script that checks timestamps. Only overwrites if the incoming event is newer. Prevents out-of-order events from replacing fresh data with stale data.

**Parallel embedding** — The Glue job uses `ThreadPoolExecutor(max_workers=10)` to call Bedrock Titan in parallel within each micro-batch.

**AgentCore Gateway over Bedrock Knowledge Base** — Gateway exposes the vector store as an MCP tool. Cleaner integration for self-managed indexes — no Knowledge Base abstraction needed.

**Consumption-based agent pricing** — AgentCore Runtime only charges for active CPU time. During I/O waits, you're not billed.

**Dev/test OpenSearch mode** — 1 OCU (no redundancy) keeps demo costs at ~$0.24/hr.

---

## Cost

### Demo (4-hour session)

| Service | Cost |
|---|---|
| OpenSearch Serverless (1 OCU) | $0.96 |
| Glue Streaming (2 DPU) | $3.52 |
| Kinesis + Bedrock + AgentCore | ~$0.40 |
| **Total** | **~$5** |

### Production (~1,000 events/min, 24/7)

| Service | Monthly |
|---|---|
| OpenSearch Serverless (2 OCU) | ~$350 |
| Glue Streaming (2-10 DPU) | ~$640 |
| Kinesis + Bedrock embeddings | ~$50-80 |
| AgentCore Runtime | Usage-dependent |
| **Total** | **~$1,000–1,400** |

---

## Cleanup

**Do this when you're done — OpenSearch Serverless charges ~$175/mo if left running.**

```bash
# 1. Destroy AgentCore resources
agentcore destroy --force

agentcore gateway delete-mcp-gateway-target \
  --name RealTimeRAGGateway \
  --target-name VectorSearchTarget

agentcore gateway delete-mcp-gateway \
  --name RealTimeRAGGateway

# 2. Delete the Gateway IAM role
aws iam delete-role-policy --role-name AgentCoreGatewayRole --policy-name InvokeLambda
aws iam delete-role --role-name AgentCoreGatewayRole

# 3. Stop the Glue job
aws glue stop-job-run --job-name realtime-rag-streaming-job --run-id YOUR_RUN_ID

# 4. Destroy all CDK resources
cdk destroy RealTimeRagAgentCoreStack
```

Verify in the AWS console that the OpenSearch Serverless collection is gone.

---

## Next Steps

- [ ] Add **AgentCore Memory** for cross-session agent learning
- [ ] Implement **AgentCore Policy** with Cedar rules for tool access control
- [ ] Tighten micro-batch window to 5s and benchmark freshness vs. cost
- [ ] Add DynamoDB metadata store for exact lookups alongside semantic search
- [ ] Multi-region deployment with Kinesis cross-region replication

---

## Author

**Stephen Daves**

- Email: [contact@stephendaves.com](mailto:contact@stephendaves.com)
- GitHub: [@sdaves](https://github.com/sdaves)

---

## License

Released under the MIT License. See [LICENSE](LICENSE).
