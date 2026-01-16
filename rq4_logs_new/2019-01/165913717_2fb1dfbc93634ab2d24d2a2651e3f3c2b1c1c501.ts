import * as aws from "@pulumi/aws";
import * as awsinfra from "@pulumi/aws-infra";
import * as pulumi from "@pulumi/pulumi";
import * as eks from "@pulumi/eks";

// Get configuration for the stack
const config = new pulumi.Config();
const instanceType = config.get("instanceType") as aws.ec2.InstanceType;
const desiredCapacity = config.getNumber("desiredCapacity");
const minSize = config.getNumber("minSize");
const maxSize = config.getNumber("maxSize");
const storageClass = config.get("storageClass") as eks.EBSVolumeType;
const deployDashboard = config.getBoolean("deployDashboard");
const identityStackName = config.require("identityStackName");

// Create a reference to the identity stack.
const identityStack = new pulumi.StackReference("identityStack", { name: identityStackName });

// Get the identity stack's outputs.
const infraRoleArn = identityStack.getOutput("infraRoleArn");
const appRoleArn = identityStack.getOutput("appRoleArn");

// Create a VPC for our cluster and other resources.
const vpc = new awsinfra.Network("vpc");

// Create an EKS cluster with the given configuration.
const cluster = new eks.Cluster(
    "cluster",
    {
        vpcId: vpc.vpcId,
        subnetIds: vpc.subnetIds,
        instanceType: instanceType,
        desiredCapacity: desiredCapacity,
        minSize: minSize,
        maxSize: maxSize,
        storageClasses: storageClass,
        deployDashboard: deployDashboard,
        roleMappings: [
            {
                roleArn: infraRoleArn,
                username: infraRoleArn,
                groups: ["system:masters"],
            },
            {
                roleArn: appRoleArn,
                username: appRoleArn,
                groups: ["system:masters"],
            },
        ],
    }
);

// Export the cluster's kubeconfig.
export const kubeconfig = cluster.kubeconfig;

const mySQL = {
    engine: "mysql",
    port: 3306,
};

const dbCommon = {
    engine: mySQL.engine,
    port: mySQL.port,
    subnetGroup: new aws.rds.SubnetGroup("dbSubnetGroup".toLowerCase(), {
        subnetIds: vpc.subnetIds,
    }),
    securityGroup: new aws.ec2.SecurityGroup("dbSecurityGroup".toLowerCase(), {
        vpcId: vpc.vpcId,
        ingress: [
            {
                fromPort: mySQL.port,
                toPort: mySQL.port,
                protocol: "TCP",
                cidrBlocks: [
                    "0.0.0.0/0",
                ],
                ipv6CidrBlocks: [
                    "::/0"
                ],
            },
        ],
    }),
};

// Create a database for the blog.
const blogDatabase = new aws.rds.Instance("blogDatabase".toLowerCase(), {
    engine: dbCommon.engine,
    port: dbCommon.port,
    name: "blog",
    username: config.require("blogDatabaseUsername"),
    password: config.require("blogDatabasePassword"),
    instanceClass: "db.t2.small",
    allocatedStorage: 20,
    vpcSecurityGroupIds: [ dbCommon.securityGroup.id ],
    dbSubnetGroupName: dbCommon.subnetGroup.id,
    finalSnapshotIdentifier: new aws.s3.Bucket("blogDatabaseFinalSnapshot").id
});

// Export the database configuration for use by applications.
export let dbConfig = {
    blog: {
        client: dbCommon.engine,
        host: blogDatabase.endpoint.apply(endpoint => endpoint.replace(":3306", "")),
        port: dbCommon.port,
        user: blogDatabase.username,
        password: blogDatabase.password,
        database: blogDatabase.name,
    }
};