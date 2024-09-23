from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_iam as iam,
    aws_glue as glue,
    CfnOutput
)
from constructs import Construct

class Usecase1Stack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create the SNS Topic for notifications
        sns_topic = sns.Topic(self, "S3UploadNotificationCDKTopic",
            display_name="S3 Upload Notifications created by CDK"
        )

        # Subscribe an email (replace with your email) to the SNS topic
        sns_topic.add_subscription(subscriptions.EmailSubscription("a.rudrabatla@nitcoinc.in"))

        # Create the S3 bucket with EventBridge notifications enabled
        source_bucket = s3.Bucket(self, "DataSourceBucket",
            bucket_name="source-bucky-2009-apsouth1",
            event_bridge_enabled=True
        )
        transformed_bucket = s3.Bucket(self, "TransformedBucket", 
            bucket_name="transform-bucky-2009-apsouth1"
        )
        
        # Create IAM Role for Step Functions
        step_function_role = iam.Role(self, "CDKStepFunctionRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            description="Role for Step Functions to publish to SNS and Start Glue Crawler"
        )
        
        # Permissions for Step Functions to publish to SNS
        step_function_role.add_to_policy(iam.PolicyStatement(
            actions=["sns:Publish"],
            resources=[sns_topic.topic_arn]
        ))
        
        # Create IAM role for Glue
        glue_role = iam.Role(
            self,
            "CDKGlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3FullAccess"
                )
            ],
            role_name="CDKGlueRoleName",
        )
        # glue_role.add_to_policy(
        #     iam.PolicyStatement(
        #         actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
        #         resources=[
        #             source_bucket.bucket_arn,
        #             f"{source_bucket.bucket_arn}/*",
        #             transformed_bucket.bucket_arn,
        #             f"{transformed_bucket.bucket_arn}/*",
        #         ],
        #     )
        # )
        
        # Create Glue Database
        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input={
                "name": "db_usecase",
                "description": "A Glue database for storing metadata",
            },
        )

        # Create Glue Crawler
        glue_crawler = glue.CfnCrawler(
            self,
            "GlueCrawler",
            role=glue_role.role_arn,
            database_name=glue_database.ref,
            name="crawl_on_usecase",
            targets={
                "s3Targets": [
                    {
                        "path": source_bucket.bucket_name  # Using source bucket as the data source
                    }
                ]
            },
            table_prefix="demo_",  # Prefix for tables created by the crawler
            description="Crawler to crawl data from the source bucket",
        )
        
        etl_script_location = "s3://etl-script-bucky-2009-apsouth1/etl_script.py"
        
        # Create Glue ETL job
        glue_job = glue.CfnJob(
            self,
            "GlueEtlJob",
            role=glue_role.role_arn,
            command={
                "name": "glueetl",  
                "scriptLocation": etl_script_location,  
                "pythonVersion": "3"  
            },
            default_arguments={
                "--job-bookmark-option": "job-bookmark-enable",  
                "--enable-metrics": "",  
                "--source_bucket": source_bucket.bucket_name,  
                "--destination_bucket": transformed_bucket.bucket_name 
            },
            max_retries=1,
            timeout=10,
            glue_version="3.0",
            number_of_workers=2,
            worker_type="Standard",
            name="my-Etl-Job",
        )
        
        # Permissions for Step Functions to start the Glue Crawler and ETL job
        step_function_role.add_to_policy(iam.PolicyStatement(
        actions=[
        "glue:StartCrawler",
        "glue:GetCrawler",
        "glue:StartJobRun",  # Permission to start the Glue ETL job
        "glue:GetJobRun",     # Permission to get job run status
        "glue:GetJob"
        ],
        resources=[
        f"arn:aws:glue:{self.region}:{self.account}:crawler/{glue_crawler.name}",
        f"arn:aws:glue:{self.region}:{self.account}:job/{glue_job.name}",
        f"arn:aws:glue:{self.region}:{self.account}:catalog"  # General permissions for the Glue catalog
        ]
        ))
            
        
        # Define Step Function tasks for Glue Crawler and ETL job
        start_crawler_task = tasks.CallAwsService(
            self, "StartCrawlerTask",
            service="Glue",
            action="startCrawler",
            parameters={
                "Name": glue_crawler.name
            },
            iam_resources=["*"],
            result_path="$.crawler"
        )

        start_etl_job_task = tasks.GlueStartJobRun(
            self, "StartETLJobTask",
            glue_job_name=glue_job.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # Enables .sync integration
            arguments=sfn.TaskInput.from_object({
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "",
                "--source_bucket": source_bucket.bucket_name,
                "--destination_bucket": transformed_bucket.bucket_name
            }),
            result_path="$.etl_job"
        )

        # Define a parallel state for Glue tasks
        parallel = sfn.Parallel(self, "ParallelExecution")
        parallel.branch(start_crawler_task)
        parallel.branch(start_etl_job_task)

        # Create a task to publish SNS notification after completion
        publish_sns_task = tasks.SnsPublish(self, "PublishSNSTask",
            topic=sns_topic,
            message=sfn.TaskInput.from_text("The Glue ETL job and Crawler have completed successfully!")
        )

        # Define overall state machine flow
        state_machine_definition = parallel.next(publish_sns_task)

        # Create the Step Function
        state_machine = sfn.StateMachine(self, "StateMachine",
            definition=state_machine_definition,
            timeout=Duration.minutes(15),
            role=step_function_role
        )

        # EventBridge Rule for S3 bucket events
        rule = events.Rule(self, "S3ObjectCreatedRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                   "bucket": {
                        "name": [source_bucket.bucket_name]
                    }
                },
            )
        )

        # Add the Step Function as the target for the EventBridge Rule
        rule.add_target(targets.SfnStateMachine(state_machine))

        # Grant EventBridge permission to invoke the Step Function
        state_machine.grant_start_execution(iam.ServicePrincipal("events.amazonaws.com"))
        
        # Outputs
        CfnOutput(self, "SourceBucketArn", value=source_bucket.bucket_arn)
        CfnOutput(self, "TransformedBucketArn", value=transformed_bucket.bucket_arn)
        CfnOutput(self, "StepFunctionRole", value=step_function_role.role_arn)
        CfnOutput(self, "GlueRoleArn", value=glue_role.role_arn)
        CfnOutput(self, "GlueDatabaseName", value=glue_database.ref)