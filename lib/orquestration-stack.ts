import { Duration, Fn, Stack, StackProps } from "aws-cdk-lib";
import { Table } from "aws-cdk-lib/aws-dynamodb";
import { Rule, Schedule } from "aws-cdk-lib/aws-events";
import { SfnStateMachine } from "aws-cdk-lib/aws-events-targets";
import { CfnJob } from "aws-cdk-lib/aws-glue";
import { PolicyStatement, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { JsonPath, StateMachine, TaskInput, Map as SfnMap } from "aws-cdk-lib/aws-stepfunctions";
import { CallAwsService, GlueStartJobRun } from "aws-cdk-lib/aws-stepfunctions-tasks";
import { Construct } from "constructs";

interface OrquestrationStackProps extends StackProps {
    glueJobs: CfnJob[]
}

export class OrquestrationStack extends Stack {
    constructor(scope: Construct, id: string, props: OrquestrationStackProps) {
        super(scope, id, props)
        const { glueJobs } = props

        const devicesTable = Table.fromTableName(this, "DevicesTable", "devices")
        const [glueJob] = glueJobs.filter((item) => item.name?.endsWith("task3"))

        const stateMachineRole = new Role(this, 'StateMachineRole', {
            assumedBy: new ServicePrincipal('states.amazonaws.com'),
        });

        devicesTable.grantReadData(stateMachineRole);

        const glueJobArn = Fn.join('', [
            'arn:aws:glue:',
            process.env.REGION as string,
            ':',
            process.env.ACCOUNT_ID as string,
            ':job/',
            glueJob.ref
        ]);

        stateMachineRole.addToPolicy(new PolicyStatement({
            actions: ['glue:StartJobRun'],
            resources: [glueJobArn],
        }));

        const scanTableTask = new CallAwsService(this, "ScanDatabase", {
            service: "dynamodb",
            action: "scan",
            parameters: {
                TableName: devicesTable.tableName,
            },
            iamResources: [devicesTable.tableArn],
            outputPath: "$.Items",
        });

        const runGlueJob = new GlueStartJobRun(this, 'RunGlueJob', {
            glueJobName: glueJob.ref,
            arguments: TaskInput.fromObject({
                '--device_id': JsonPath.stringAt('$.device_id.S'),
            }),
            resultPath: '$.jobResult'
        });

        const iterateItemsInParallel = new SfnMap(this, 'IterateItemsInParallel', {
            inputPath: '$',
            itemsPath: '$',
            maxConcurrency: 5,
        });

        iterateItemsInParallel.itemProcessor(runGlueJob);
        const definition = scanTableTask.next(iterateItemsInParallel);

        const stateMachine = new StateMachine(this, 'StateMachine', {
            definition,
            timeout: Duration.minutes(10),
            role: stateMachineRole,
        });

        const rule = new Rule(this, 'ScheduleRule', {
            schedule: Schedule.cron({ minute: '30', hour: '2', weekDay: 'MON,FRI' }),
        });

        // Add the Step Function as the target of the rule
        rule.addTarget(new SfnStateMachine(stateMachine));
    }
}