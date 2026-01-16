import { App, Stack } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { SecureLogBucket } from '../src';

describe('SecureLogBucket specific Testing', () => {

  const stack = new Stack(new App(), 'TestingStack', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
  });

  const bucket = new SecureLogBucket(stack, 'SecureLogBucket', {
    bucketName: 'example-log-bucket',
    changeClassTransition: {
      infrequentAccessDays: 20,
      intelligentTieringDays: 40,
      glacierDays: 60,
      deepArchiveDays: 80,
    },
  });

  it('Is Bucket', () => {
    expect(bucket).toBeInstanceOf(s3.Bucket);
  });

  const template = Template.fromStack(stack);

  it('Should match lifecycle', () => {
    template.hasResourceProperties('AWS::S3::Bucket', {
      LifecycleConfiguration: {
        Rules: Match.arrayEquals([
          Match.objectEquals({
            Id: 'archive-step-lifecycle-rule',
            Status: 'Enabled',
            Transitions: Match.arrayEquals([
              Match.objectEquals({
                StorageClass: 'STANDARD_IA',
                TransitionInDays: 20,
              }),
              Match.objectEquals({
                StorageClass: 'INTELLIGENT_TIERING',
                TransitionInDays: 40,
              }),
              Match.objectEquals({
                StorageClass: 'GLACIER',
                TransitionInDays: 60,
              }),
              Match.objectEquals({
                StorageClass: 'DEEP_ARCHIVE',
                TransitionInDays: 80,
              }),
            ]),
          }),
        ]),
      },
    });
  });

  it('Should match snapshot', () => {
    expect(template.toJSON()).toMatchSnapshot();
  });

});