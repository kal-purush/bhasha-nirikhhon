using Amazon.CDK;
using Amazon.CDK.AWS.ECR;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.S3;

using Constructs;

using EcrLifecycleRule = Amazon.CDK.AWS.ECR.LifecycleRule;

namespace Brighid.Discord.Adapter.Artifacts
{
    /// <summary>
    /// Stack that contains repositories for storing artifacts.
    /// </summary>
    public class ArtifactsStack : Stack
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ArtifactsStack" /> class.
        /// </summary>
        /// <param name="scope">The scope to create this artifacts stack in.</param>
        /// <param name="id">The ID of the Artifacts Stack.</param>
        /// <param name="props">The props for the Artifacts Stack.</param>
        public ArtifactsStack(Construct scope, string id, IStackProps? props = null)
            : base(scope, id, props)
        {
            AddRepository();
            AddBucket();
        }

        private void AddBucket()
        {
            var bucket = new Bucket(this, "Bucket");
            bucket.ApplyRemovalPolicy(RemovalPolicy.DESTROY);
            bucket.AddToResourcePolicy(new PolicyStatement(new PolicyStatementProps
            {
                Effect = Effect.ALLOW,
                Actions = new[] { "s3:*Object" },
                Resources = new[] { bucket.BucketArn, $"{bucket.BucketArn}/*" },
                Principals = new[]
                {
                    new AccountPrincipal(Fn.Ref("AWS::AccountId")),
                    new ArnPrincipal(Fn.ImportValue("cfn-metadata:DevAgentRoleArn")),
                    new ArnPrincipal(Fn.ImportValue("cfn-metadata:DevAgentRoleArn")),
                },
            }));

            _ = new CfnOutput(this, "BucketName", new CfnOutputProps
            {
                Value = bucket.BucketName,
                Description = "Name of the Artifacts Bucket for Brighid Discord Adapter.",
            });
        }

        private void AddRepository()
        {
            var repository = new Repository(this, "ImageRepository", new RepositoryProps
            {
                ImageScanOnPush = true,
                LifecycleRules = new[]
                {
                    new EcrLifecycleRule
                    {
                        Description = "Protect prod-tagged images.",
                        RulePriority = 1,
                        TagStatus = TagStatus.TAGGED,
                        TagPrefixList = new[] { "prod" },
                        MaxImageCount = 1,
                    },
                    new EcrLifecycleRule
                    {
                        Description = "Protect dev-tagged images.",
                        RulePriority = 2,
                        TagStatus = TagStatus.TAGGED,
                        TagPrefixList = new[] { "dev" },
                        MaxImageCount = 1,
                    },
                    new EcrLifecycleRule
                    {
                        Description = "Keep last 3 images not tagged with dev or prod",
                        RulePriority = 3,
                        TagStatus = TagStatus.ANY,
                        MaxImageCount = 3,
                    },
                },
            });

            var policyStatement = new PolicyStatement(new PolicyStatementProps
            {
                Effect = Effect.ALLOW,
                Actions = new[]
                {
                    "ecr:GetAuthorizationToken",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:ListImages",
                },
                Principals = new[]
                {
                    new AccountPrincipal(Fn.Ref("AWS::AccountId")),
                    new AccountPrincipal(Fn.ImportValue("cfn-metadata:DevAccountId")),
                    new AccountPrincipal(Fn.ImportValue("cfn-metadata:ProdAccountId")),
                },
            });

            repository.ApplyRemovalPolicy(RemovalPolicy.DESTROY);
            repository.AddToResourcePolicy(policyStatement);

            _ = new CfnOutput(this, "ImageRepositoryUri", new CfnOutputProps
            {
                Value = repository.RepositoryUri,
                Description = "URI of the container image repository for Brighid Discord Adapter.",
            });
        }
    }
}