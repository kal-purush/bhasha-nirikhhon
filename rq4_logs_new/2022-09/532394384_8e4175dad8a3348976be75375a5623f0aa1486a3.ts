import { Certificate, CertificateValidation } from 'aws-cdk-lib/aws-certificatemanager';
import { Construct } from 'constructs';
import { HostedZone, ARecord, RecordTarget, CnameRecord, NsRecord } from 'aws-cdk-lib/aws-route53';
import { Metric } from 'aws-cdk-lib/aws-cloudwatch';
import { Route53RecordTarget } from 'aws-cdk-lib/aws-route53-targets';
import { Stack, StackProps, Duration, CfnOutput, RemovalPolicy } from 'aws-cdk-lib';

import { Config } from '../config';

interface DNSProps extends StackProps {
  config: Config;
}

export class DNS extends Stack {
  readonly zone;
  constructor(scope: Construct, id: string, props: DNSProps) {
    super(scope, id, props);

    // DNS & certs
    this.zone = new HostedZone(this, props.config.zoneId, {
      zoneName: props.config.domainBase,
    });
    this.zone.applyRemovalPolicy(RemovalPolicy.DESTROY);
   
    const metric = new Metric({
      namespace: 'AWS/Route53',
      metricName: 'DNSQueries',
      dimensionsMap: {
        HostedZoneId: this.zone.hostedZoneId,
      }
    });

    // const record =vnew ARecord(this, 'AliasRecord', {
    //   zone,
    //   target: RecordTarget.fromAlias(new Route53RecordTarget(props.config.domainBase),
    //   deleteExisting: true,
    //   ttl: Duration.minutes(5),
    // });
    // record.applyRemovalPolicy(RemovalPolicy.DESTROY);

    // const cname = new CnameRecord(this, `${props.config.stage}CnameRecord`, {
    //   recordName: props.config.stage,
    //   zone,
    //   domainName: props.config.domainBase,
    //   deleteExisting: true,
    //   ttl: Duration.minutes(5),
    // });
    // cname.applyRemovalPolicy(RemovalPolicy.DESTROY);

    // const ns = new NsRecord(this, 'NSRecord', {
    //   zone,
    //   recordName: props.config.domainBase,
    //   values: [
    //   // Get these from the AWS > Route53 > Registered domains > <domain_name> > Name servers
    //     'ns-1214.awsdns-23.org.',
    //     'ns-191.awsdns-23.com.',
    //     'ns-1640.awsdns-13.co.uk.',
    //     'ns-790.awsdns-34.net.',
    //   ],
    //   deleteExisting: true,
    //   ttl: Duration.minutes(5),
    // });
    // ns.applyRemovalPolicy(RemovalPolicy.DESTROY);

    // const certificate = new Certificate(this, `${id}Certificate`, {
    //   domainName: props.config.domainBase,
    //   subjectAlternativeNames: [`www.${props.config.domainBase}`],
    //   validation: CertificateValidation.fromDns(zone),
    // });
    // certificate.applyRemovalPolicy(RemovalPolicy.DESTROY);
  }
}