import { Construct } from "@aws-cdk/core";
import {
  IVpc,
  ISecurityGroup,
} from "@aws-cdk/aws-ec2";
import {
  CfnCacheCluster,
  CfnSubnetGroup
} from "@aws-cdk/aws-elasticache";

interface ElastiCacheProps {
  env: string;
  vpc: IVpc;
  sg: { [key: string]: ISecurityGroup; };
}

export class ElastiCache extends Construct {
  constructor(parent: Construct, name: string, props: ElastiCacheProps) {
    super(parent, name);

    const subnets = props.vpc.privateSubnets.map(subnet => subnet.subnetId)
    const subnetGroup = new CfnSubnetGroup(this, `${props.env}-subnet`, {
      cacheSubnetGroupName: `${props.env}-subnet`,
      description: `${props.env} subnet`,
      subnetIds: subnets
    })


    const redis = new CfnCacheCluster(this, `${props.env}-redis-1`, {
      azMode: "single-az",
      cacheNodeType: "cache.t3.micro",
      cacheSubnetGroupName: `${props.env}-subnet`,
      clusterName: `${props.env}-redis-1`,
      engine: "redis",
      engineVersion: "5.0.6",
      numCacheNodes: 1,
      vpcSecurityGroupIds: [props.sg["redis"].securityGroupId]
    })
  }
}