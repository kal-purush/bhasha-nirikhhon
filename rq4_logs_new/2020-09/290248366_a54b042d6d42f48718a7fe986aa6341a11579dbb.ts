import cdk = require("@aws-cdk/core");
import ec2 = require("@aws-cdk/aws-ec2");
import elb = require("@aws-cdk/aws-elasticloadbalancingv2");
import asg = require("@aws-cdk/aws-autoscaling");
import acm = require("@aws-cdk/aws-certificatemanager");
import r53 = require('@aws-cdk/aws-route53')

interface ElbProps {
  vpc:       ec2.IVpc;
  asgs:      { [key: string]: asg.AutoScalingGroup };
}

export class Elb extends cdk.Construct {
  public readonly albs:      { [key: string]: elb.IApplicationLoadBalancer } = {};

  constructor(parent: cdk.Construct, name: string, props: ElbProps) {
    super(parent, name);
    const env:      string  = this.node.tryGetContext('env');
    const hostzone: string  = this.node.tryGetContext('hostzone');
    const public_zone       = r53.HostedZone.fromLookup(this, hostzone, { domainName: hostzone })
    const private_subnet    = { subnetType: ec2.SubnetType.PRIVATE }
    const public_subnet     = { subnetType: ec2.SubnetType.PUBLIC }

    let web_tg = new elb.ApplicationTargetGroup(this, `${env}-web-tg`, {
      vpc: props.vpc,
      targetType: elb.TargetType.INSTANCE,
      targetGroupName: `${env}-web-tg`,
      port: 4567,
      protocol: elb.ApplicationProtocol.HTTP,
      targets: [props.asgs["web"]]
    })

    // create loadbalancer
    this.albs["web"] = new elb.ApplicationLoadBalancer(this, `${env}-web-alb`, {
      vpc: props.vpc,
      internetFacing: true,
      vpcSubnets: public_subnet,
    })

    this.albs["web"].connections.allowDefaultPortFromAnyIpv4

    // add listener
    let web_alb_https = this.albs["web"].addListener('web-alb-https', {
      port: 443,
      defaultTargetGroups: [web_tg],
      certificateArns: [
        new acm.DnsValidatedCertificate(this, 'SiteCertificate', {
          domainName: `www.${hostzone}`,
          hostedZone: public_zone,
          region: 'ap-northeast-1',
        }).certificateArn
      ],
    })
    //web_alb_https.addTargets("web", {
    //  port: 4567,
    //  protocol: elb.ApplicationProtocol.HTTP,
    //  priority: 1,
    //  hostHeader: `www.${hostzone}`,
    //  targetGroupName: `${env}-web-tg`
    //});
  }
}