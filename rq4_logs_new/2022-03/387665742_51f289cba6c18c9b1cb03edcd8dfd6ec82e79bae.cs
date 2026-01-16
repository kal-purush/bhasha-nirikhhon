using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Amazon.CDK;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.S3;

using Brighid.Commands.Cicd.Utils;

namespace Brighid.Commands.Artifacts
{
    /// <summary>
    /// Stack that contains repositories for storing artifacts.
    /// </summary>
    public class ArtifactsStack : Stack
    {
        private const string Name = "brighid-commands-resources-cicd";
        private static readonly string OutputDirectory = ProjectRootDirectoryAttribute.ThisAssemblyProjectRootDirectory + "obj/Cicd.Artifacts/cdk.out";

        private static readonly App App = new(new AppProps
        {
            Outdir = OutputDirectory,
        });

        private static readonly StackProps Props = new()
        {
            Synthesizer = new BootstraplessSynthesizer(new BootstraplessSynthesizerProps()),
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="ArtifactsStack" /> class.
        /// </summary>
        public ArtifactsStack()
            : base(App, Name, Props)
        {
            AddBucket();
        }

        /// <summary>
        /// Deploys the artifacts stack.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel the operation.</param>
        /// <returns>The stack outputs.</returns>
        public static async Task<ArtifactsStackOutputs> Deploy(CancellationToken cancellationToken)
        {
            _ = new ArtifactsStack();
            var result = App.Synth();
            var deployer = new StackDeployer();
            var templateFile = result.Stacks.First().TemplateFullPath;
            var templateBody = await File.ReadAllTextAsync(templateFile, cancellationToken);

            var context = new DeployContext { StackName = Name, TemplateBody = templateBody };
            var outputs = await deployer.Deploy(context, cancellationToken);

            return new ArtifactsStackOutputs
            {
                BucketName = outputs[nameof(ArtifactsStackOutputs.BucketName)],
            };
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
                },
            }));

            bucket.AddToResourcePolicy(new PolicyStatement(new PolicyStatementProps
            {
                Effect = Effect.ALLOW,
                Actions = new[] { "s3:GetObject" },
                Resources = new[] { bucket.BucketArn, $"{bucket.BucketArn}/*" },
                Principals = new[]
                {
                    new ArnPrincipal(Fn.ImportValue("cfn-metadata:DevAccountId")),
                    new ArnPrincipal(Fn.ImportValue("cfn-metadata:ProdAccountId")),
                },
            }));

            _ = new CfnOutput(this, nameof(ArtifactsStackOutputs.BucketName), new CfnOutputProps
            {
                Value = bucket.BucketName,
                Description = "Name of the Artifacts Bucket for Brighid Commands.",
            });
        }
    }
}