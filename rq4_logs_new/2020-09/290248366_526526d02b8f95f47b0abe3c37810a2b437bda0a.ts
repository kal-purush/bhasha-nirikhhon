import cdk = require('@aws-cdk/core');
import { Construct } from "@aws-cdk/core";
import {
  Instance,
  InstanceType,
  AmazonLinuxImage,
  Peer,
  Port,
  ISecurityGroup,
  IVpc
} from "@aws-cdk/aws-ec2";

interface ComputeStackProps {
  env: string;
  vpc: IVpc;
  sg: { [key: string]: ISecurityGroup; };
}

export class Compute extends Construct {
  public readonly nodes: { [key: string]: Instance; } = {};

  constructor(parent: Construct, name: string, props: ComputeStackProps) {
    super(parent, name);

    // create mgmt instance
    this.nodes["bastion"] = new Instance(this, `${props.env}-bastion`, {
      vpc: props.vpc,
      vpcSubnets: { subnetName: `${props.env}-public` },
      instanceType: new InstanceType("t3a.micro"),
      machineImage: new AmazonLinuxImage(),
      securityGroup: props.sg['bastion'],
      keyName: "mgmt"
    });

    // add rule for mgmt-instance
    props.sg["private-app"].addIngressRule(
      Peer.ipv4(this.nodes["bastion"].instancePrivateIp + "/32"),
      Port.tcp(22),
      "allow ssh for bastion"
    );

    // create instance
    this.nodes["redis-cli"] = new Instance(this, `${props.env}-redis-cli`, {
      vpc: props.vpc,
      vpcSubnets: { subnetName: `${props.env}-private` },
      instanceType: new InstanceType("t3a.micro"),
      machineImage: new AmazonLinuxImage(),
      securityGroup: props.sg['private-app'],
      keyName: "bastion"
    });
  }
}