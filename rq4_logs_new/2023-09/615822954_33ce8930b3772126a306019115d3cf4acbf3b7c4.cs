using Amazon.CDK.AWS.EC2;
using Amazon.CDK.AWS.ECS;
using Amazon.CDK.AWS.EFS;
using Amazon.CDK.AWS.Logs;
using Amazon.CDK;
using Constructs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Valheim
{
    public class ValheimWorldProps
    {
        public string Name { get; set; }
        public string World { get; set; }
        public string Password { get; set; }
    }
    internal class ValheimWorld : Construct
    {   
        public ValheimWorld(Construct scope, string id, ValheimWorldProps props) : base(scope, id)
        {
            var vpc = Vpc.FromLookup(this, "DefaultVpc", new VpcLookupOptions
            {
                IsDefault = true
            });

            var fileSystem = new Amazon.CDK.AWS.EFS.FileSystem(this, "ValheimFileSystem", new FileSystemProps
            {
                Vpc = vpc,
                RemovalPolicy = RemovalPolicy.RETAIN,
                Encrypted = false
            });

            var volumeConfig = new Amazon.CDK.AWS.ECS.Volume
            {
                Name = "ValheimVolume",
                EfsVolumeConfiguration = new EfsVolumeConfiguration
                {
                    FileSystemId = fileSystem.FileSystemId
                }
            };            

            var taskDefinition = new FargateTaskDefinition(this, "ValheimTask", new FargateTaskDefinitionProps
            {
                Family = "valheim",
                Volumes = new[] { volumeConfig },
                Cpu = 1024,
                MemoryLimitMiB = 2048
            });

            var containerDefinition = taskDefinition.AddContainer("ValheimContainer", new ContainerDefinitionProps
            {
                Image = ContainerImage.FromRegistry("lloesche/valheim-server"),
                Logging = LogDriver.AwsLogs(new AwsLogDriverProps
                {
                    StreamPrefix = "ValheimContainer",
                    LogRetention = RetentionDays.ONE_DAY
                }),
                Environment = new Dictionary<string, string>
                {
                    { "SERVER_PORT", "2456" },
                    { "SERVER_NAME", props.World },
                    { "WORLD_NAME", props.Name },
                    { "SERVER_PASS", props.Password },
                    { "SERVER_ARGS", "-crossplay" }
                }
            });

            containerDefinition.AddMountPoints(new MountPoint
            {
                ContainerPath = "/config",
                SourceVolume = volumeConfig.Name,
                ReadOnly = false
            });

            var cluster = new Cluster(this, "ValheimCluster", new ClusterProps
            {
                Vpc = vpc
            });

            var service = new FargateService(this, "ValheimService", new FargateServiceProps
            {
                Cluster = cluster,
                AssignPublicIp = true,
                TaskDefinition = taskDefinition,
                DesiredCount = 1
            });

            // Allow TCP 2049 for EFS
            service.Connections.AllowFrom(fileSystem, Port.Tcp(2049));
            service.Connections.AllowTo(fileSystem, Port.Tcp(2049));

            // Allow UDP 2456-2458 for Valheim
            service.Connections.AllowFromAnyIpv4(Port.UdpRange(2456, 2458), "Allow Valheim Traffic");
        }
    }
}