import * as cdk from "aws-cdk-lib";
import * as ecs from "aws-cdk-lib/aws-ecs";
import { Template, Match } from "aws-cdk-lib/assertions";
import { FargateBatchWithS3Buckets } from "../lib/fargate-batch-with-s3";

test("Looks about right", () => {
  const app = new cdk.App();
  const stack = new cdk.Stack(app, "TestStack");

  new FargateBatchWithS3Buckets(stack, "TestConstruct", {
    containerImage: ecs.ContainerImage.fromRegistry("amazonlinux"),
    filters: [{ suffix: ".bam" }],
  });

  const template = Template.fromStack(stack);

  template.resourceCountIs("AWS::S3::Bucket", 2);
  template.resourceCountIs("AWS::Batch::ComputeEnvironment", 1);
  template.resourceCountIs("AWS::Batch::JobQueue", 1);
  template.resourceCountIs("AWS::Batch::JobDefinition", 1);
  template.hasResourceProperties("AWS::Lambda::Function", {
    Handler: "bucketarrival.handler",
  });
  template.hasResourceProperties("AWS::IAM::Policy", {
    PolicyDocument: {
      Statement: [
        {
          Action: "batch:SubmitJob",
          Effect: "Allow",
        },
      ],
    },
  });
});