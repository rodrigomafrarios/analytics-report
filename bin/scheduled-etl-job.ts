#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { ScheduledEtlJobStack } from '../lib/scheduled-etl-job-stack';
import { OrquestrationStack } from '../lib/orquestration-stack';

const app = new cdk.App();
const commomProps = {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION
  }
}

const { glueJobs } = new ScheduledEtlJobStack(app, 'ScheduledEtlJobStack', {
  ...commomProps,
});

new OrquestrationStack(app, 'OrquestrationStack', {
  ...commomProps,
  glueJobs
})

