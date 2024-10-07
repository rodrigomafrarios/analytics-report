import { RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { AttributeType, Table } from "aws-cdk-lib/aws-dynamodb";
import { CfnJob, CfnTrigger } from "aws-cdk-lib/aws-glue";
import { Role, ServicePrincipal, ManagedPolicy } from "aws-cdk-lib/aws-iam";
import { BlockPublicAccess, Bucket, BucketEncryption } from "aws-cdk-lib/aws-s3";
import { BucketDeployment, Source } from "aws-cdk-lib/aws-s3-deployment";
import { Construct } from "constructs";
import { readdirSync } from "fs";
import { join } from "path";

export class ScheduledEtlJobStack extends Stack {

  readonly glueJobs: CfnJob[] = []

  constructor(scope: Construct, id: string, props: StackProps) {
    super(scope, id, props)

    const devicesTable = new Table(this, "Devices", {
      tableName: "devices",
      partitionKey: {
        name: "device_id",
        type: AttributeType.STRING
      },
      removalPolicy: RemovalPolicy.RETAIN
    })

    const sourceBucket = Bucket.fromBucketName(this, "SourceBucket", "BUCKET_NAME")

    const glueAssetsBucket = new Bucket(this, `GlueAssetsBucket`, {
      bucketName: `scheduled-etl-job-assets`,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      encryption: BucketEncryption.S3_MANAGED,
      versioned: false,
      enforceSSL: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    new BucketDeployment(this, "PythonScripts", {
      sources: [Source.asset(join(__dirname, "../script"))],
      destinationBucket: glueAssetsBucket
    });

    const outputBucket = new Bucket(this, `QueryOutputBucket`, {
      bucketName: `glue-job-query-output`,
      publicReadAccess: false,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      encryption: BucketEncryption.S3_MANAGED,
      versioned: false,
      enforceSSL: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const jobRole = new Role(this, 'JobRole', {
      assumedBy: new ServicePrincipal('glue'),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });

    sourceBucket.grantRead(jobRole)
    outputBucket.grantReadWrite(jobRole)
    glueAssetsBucket.grantRead(jobRole)
    devicesTable.grantReadData(jobRole)

    const scripts = readdirSync(join(__dirname, "../script"), { encoding: "utf-8" })

    let concurrencyConfig = undefined

    for (const script of scripts) {

      if (script.startsWith("task3")) {
        concurrencyConfig = {
          executionProperty: {
            maxConcurrentRuns: 4
          }
        }
      }

      const task = script.split("-")[0]

      const glueJob = new CfnJob(this, `Job-${id}-${task}`, {
        glueVersion: '4.0',
        name: `scheduled-etl-job-${task}`,
        role: jobRole.roleArn,
        command: {
          name: 'glueetl',
          pythonVersion: '3',
          scriptLocation: `s3://${glueAssetsBucket.bucketName}/${script}`,
        },
        jobMode: "VISUAL",
        defaultArguments: {
          '--job-bookmark-option': 'job-bookmark-disable',
          '--enable-metrics': 'true',
          '--enable-continuous-cloudwatch-log': 'true',
          '--enable-observability-metrics': 'true',
          "--enable-glue-datacatalog": 'true',
          "--enable-auto-scaling": 'true',
        },
        timeout: 60,
        ...concurrencyConfig
      })

      this.glueJobs.push(glueJob);

      new CfnTrigger(this, `JobTrigger-${id}-${task}`, {
        type: "SCHEDULED",
        // startOnCreation: true,
        schedule: "cron(30 2 ? * 2,6 *)",
        actions: [{ jobName: glueJob.name }]
      })
    }
  }
}