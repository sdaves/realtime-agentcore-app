#!/usr/bin/env python3
"""
Real-Time RAG Pipeline on AWS with AgentCore — CDK Stack
==========================================================
Deploys: Kinesis → Glue Streaming → OpenSearch Serverless → AgentCore Gateway + Runtime

Prerequisites:
  - CDK v2: npm install -g aws-cdk
  - Python 3.11+
  - Bedrock model access for amazon.titan-embed-text-v2:0 and anthropic.claude-3-haiku
  - AgentCore SDK: pip install bedrock-agentcore

Deploy:
  python -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt
  cdk bootstrap
  cdk deploy RealTimeRagAgentCoreStack

Estimated demo cost (4 hours): ~$5
WARNING: OpenSearch Serverless charges ~$175/mo if left running. Run `cdk destroy` after!
"""

from aws_cdk import (
    App,
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_kinesis as kinesis,
    aws_iam as iam,
    aws_glue as glue,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_opensearchserverless as oss,
    aws_lambda as _lambda,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    aws_logs as logs,
)
from constructs import Construct
import json


class RealTimeRagAgentCoreStack(Stack):
    """
    Streaming RAG pipeline with AgentCore.

    Architecture:
      Kinesis (ingest) → Glue Streaming (transform + embed) → OpenSearch Serverless (vectors)
        → Lambda (vector search wrapper) → AgentCore Gateway (MCP tool) → AgentCore Runtime (agent)
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # =====================================================================
        # 1. KINESIS DATA STREAM
        # =====================================================================
        stream = kinesis.Stream(
            self, "RealTimeRagStream",
            stream_name="realtime-rag-stream",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            retention_period=Duration.hours(24),
        )

        # =====================================================================
        # 2. S3 BUCKET — Glue scripts + dead letter storage
        # =====================================================================
        glue_bucket = s3.Bucket(
            self, "GlueJobBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        s3deploy.BucketDeployment(
            self, "DeployGlueScripts",
            sources=[s3deploy.Source.asset("./glue_scripts")],
            destination_bucket=glue_bucket,
            destination_key_prefix="scripts",
        )

        # =====================================================================
        # 3. OPENSEARCH SERVERLESS — Vector store (dev/test mode for low cost)
        # =====================================================================
        encryption_policy = oss.CfnSecurityPolicy(
            self, "OSSEncryptionPolicy",
            name="rag-vector-encryption",
            type="encryption",
            policy=json.dumps({
                "Rules": [{"ResourceType": "collection", "Resource": ["collection/rag-vectors"]}],
                "AWSOwnedKey": True,
            }),
        )

        network_policy = oss.CfnSecurityPolicy(
            self, "OSSNetworkPolicy",
            name="rag-vector-network",
            type="network",
            policy=json.dumps([{
                "Rules": [
                    {"ResourceType": "collection", "Resource": ["collection/rag-vectors"]},
                    {"ResourceType": "dashboard", "Resource": ["collection/rag-vectors"]},
                ],
                "AllowFromPublic": True,
            }]),
        )

        vector_collection = oss.CfnCollection(
            self, "VectorCollection",
            name="rag-vectors",
            type="VECTORSEARCH",
            description="Real-time RAG vector store — dev/test mode",
        )
        vector_collection.add_dependency(encryption_policy)
        vector_collection.add_dependency(network_policy)

        # =====================================================================
        # 4. IAM ROLE — Glue job
        # =====================================================================
        glue_role = iam.Role(
            self, "GlueStreamingRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
            ],
        )

        stream.grant_read(glue_role)
        glue_bucket.grant_read_write(glue_role)

        glue_role.add_to_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=[f"arn:aws:bedrock:{self.region}::foundation-model/amazon.titan-embed-text-v2:0"],
        ))

        glue_role.add_to_policy(iam.PolicyStatement(
            actions=["aoss:APIAccessAll"],
            resources=[vector_collection.attr_arn],
        ))

        # =====================================================================
        # 5. VECTOR SEARCH LAMBDA — Wraps OpenSearch k-NN for AgentCore Gateway
        # =====================================================================
        vector_search_lambda = _lambda.Function(
            self, "VectorSearchLambda",
            function_name="rag-vector-search",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=_lambda.Code.from_asset(
                "./lambda/vector_search",
                bundling={
                    "image": _lambda.Runtime.PYTHON_3_12.bundling_image,
                    "command": [
                        "bash", "-c",
                        "pip install opensearch-py requests-aws4auth -t /asset-output && cp -r . /asset-output/"
                    ],
                },
            ),
            timeout=Duration.seconds(30),
            memory_size=512,
            environment={
                "OPENSEARCH_ENDPOINT": vector_collection.attr_collection_endpoint,
                "INDEX_NAME": "rag-vectors-index",
                "EMBEDDING_MODEL_ID": "amazon.titan-embed-text-v2:0",
                "AWS_REGION_NAME": self.region,
            },
        )

        # Lambda needs Bedrock + OpenSearch access
        vector_search_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=["bedrock:InvokeModel"],
            resources=[f"arn:aws:bedrock:{self.region}::foundation-model/amazon.titan-embed-text-v2:0"],
        ))
        vector_search_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=["aoss:APIAccessAll"],
            resources=[vector_collection.attr_arn],
        ))

        # =====================================================================
        # 6. OPENSEARCH DATA ACCESS POLICY — Grant both Glue + Lambda
        # =====================================================================
        oss.CfnAccessPolicy(
            self, "OSSDataAccessPolicy",
            name="rag-vector-access",
            type="data",
            policy=json.dumps([{
                "Rules": [
                    {
                        "ResourceType": "index",
                        "Resource": ["index/rag-vectors/*"],
                        "Permission": [
                            "aoss:CreateIndex", "aoss:UpdateIndex", "aoss:DescribeIndex",
                            "aoss:ReadDocument", "aoss:WriteDocument",
                        ],
                    },
                    {
                        "ResourceType": "collection",
                        "Resource": ["collection/rag-vectors"],
                        "Permission": [
                            "aoss:CreateCollectionItems", "aoss:DescribeCollectionItems",
                            "aoss:UpdateCollectionItems",
                        ],
                    },
                ],
                "Principal": [
                    glue_role.role_arn,
                    vector_search_lambda.role.role_arn,
                ],
            }]),
        )

        # =====================================================================
        # 7. GLUE STREAMING JOB
        # =====================================================================
        glue_job = glue.CfnJob(
            self, "StreamingRagJob",
            name="realtime-rag-streaming-job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="gluestreaming",
                python_version="3",
                script_location=f"s3://{glue_bucket.bucket_name}/scripts/streaming_job.py",
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            default_arguments={
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-spark-ui": "true",
                "--TempDir": f"s3://{glue_bucket.bucket_name}/temp/",
                "--additional-python-modules": "boto3>=1.35.0,opensearch-py>=2.4.0,requests-aws4auth>=1.3.0",
                "--kinesis_stream_name": stream.stream_name,
                "--opensearch_endpoint": vector_collection.attr_collection_endpoint,
                "--opensearch_index": "rag-vectors-index",
                "--embedding_model_id": "amazon.titan-embed-text-v2:0",
                "--aws_region": self.region,
                "--window_size": "10 seconds",
            },
            timeout=2880,
            max_retries=0,
        )

        # =====================================================================
        # 8. CLOUDWATCH MONITORING
        # =====================================================================
        alarm_topic = sns.Topic(self, "PipelineAlarmTopic", topic_name="realtime-rag-alarms")

        kinesis_iterator_alarm = cw.Alarm(
            self, "KinesisIteratorAgeAlarm",
            metric=stream.metric("GetRecords.IteratorAgeMilliseconds",
                statistic="Maximum", period=Duration.minutes(1)),
            threshold=60000,
            evaluation_periods=3,
            alarm_description="Kinesis iterator age > 60s — Glue may be falling behind",
        )
        kinesis_iterator_alarm.add_alarm_action(cw_actions.SnsAction(alarm_topic))

        dashboard = cw.Dashboard(self, "PipelineDashboard", dashboard_name="RealTimeRAGPipeline")
        dashboard.add_widgets(
            cw.Row(
                cw.GraphWidget(
                    title="Kinesis — iterator age (ms)",
                    left=[stream.metric("GetRecords.IteratorAgeMilliseconds",
                        statistic="Maximum", period=Duration.minutes(1))],
                    width=12,
                ),
                cw.GraphWidget(
                    title="Kinesis — incoming records/min",
                    left=[stream.metric("IncomingRecords",
                        statistic="Sum", period=Duration.minutes(1))],
                    width=12,
                ),
            ),
            cw.Row(
                cw.GraphWidget(
                    title="Vector search Lambda — duration",
                    left=[vector_search_lambda.metric_duration(period=Duration.minutes(1))],
                    width=12,
                ),
                cw.GraphWidget(
                    title="Vector search Lambda — errors",
                    left=[vector_search_lambda.metric_errors(period=Duration.minutes(1))],
                    width=12,
                ),
            ),
        )

        # =====================================================================
        # 9. OUTPUTS
        # =====================================================================
        CfnOutput(self, "KinesisStreamName", value=stream.stream_name)
        CfnOutput(self, "OpenSearchEndpoint", value=vector_collection.attr_collection_endpoint)
        CfnOutput(self, "OpenSearchDashboard", value=vector_collection.attr_dashboard_endpoint)
        CfnOutput(self, "GlueJobName", value=glue_job.name or "realtime-rag-streaming-job")
        CfnOutput(self, "VectorSearchLambdaArn", value=vector_search_lambda.function_arn)
        CfnOutput(self, "VectorSearchLambdaName", value=vector_search_lambda.function_name)
        CfnOutput(self, "AlarmTopicArn", value=alarm_topic.topic_arn)
        CfnOutput(self, "DashboardURL",
            value=f"https://{self.region}.console.aws.amazon.com/cloudwatch/home?region={self.region}#dashboards:name=RealTimeRAGPipeline")

        # =====================================================================
        # AGENTCORE SETUP NOTES
        # =====================================================================
        # AgentCore Gateway and Runtime are configured via the AgentCore SDK,
        # not CloudFormation/CDK (as of March 2026).
        #
        # After CDK deploy, run:
        #   python setup_agentcore.py
        #
        # This script:
        #   1. Creates a Gateway target pointing to the vector search Lambda
        #   2. Registers the vector search as an MCP tool
        #   3. Deploys the agent to AgentCore Runtime
        #
        # See setup_agentcore.py for full details.


# =============================================================================
app = App()
RealTimeRagAgentCoreStack(app, "RealTimeRagAgentCoreStack")
app.synth()
