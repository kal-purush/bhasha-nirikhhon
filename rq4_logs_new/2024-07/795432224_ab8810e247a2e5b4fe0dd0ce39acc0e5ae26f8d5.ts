import { RemovalPolicy, Stack } from 'aws-cdk-lib'
import { Construct } from 'constructs'
import * as iam from 'aws-cdk-lib/aws-iam'
import * as apigw from 'aws-cdk-lib/aws-apigateway'
import * as ec2 from 'aws-cdk-lib/aws-ec2'
import * as logs from 'aws-cdk-lib/aws-logs'
import { type Stream } from 'aws-cdk-lib/aws-kinesis'

interface KdsPrivateApiGwProducerProps {
  dataStream: Stream
  /* APIGWのタイプ, Privateの場合はVPCの指定が必須 */
  type?: apigw.EndpointType.PRIVATE | apigw.EndpointType.REGIONAL
  /* Private APIの場合に指定、VPC Endpointを作成するVPC */
  vpc?: ec2.Vpc
  /* Logging, X-Rayトレーシングなど詳細モニタリング設定の有効化 */
  enableDetailMonitoring?: boolean
}

/**
 * API Gateway - KDS統合構成
 * Private APIにも対応
 * PutRcord, PutRecords APIに対応
 */
export class KdsPrivateApiGwProducer extends Construct {
  private readonly role: iam.Role
  private readonly stageOption: apigw.StageOptions
  private readonly vpcEndpoint: ec2.InterfaceVpcEndpoint
  private readonly restApi: apigw.RestApi

  constructor(scope: Construct, id: string, props: KdsPrivateApiGwProducerProps) {
    super(scope, id)

    props.type ??= apigw.EndpointType.REGIONAL
    props.enableDetailMonitoring ??= false

    /*
    * IAM
    -------------------------------------------------------------------------- */
    this.role = new iam.Role(this, 'Role', {
      assumedBy: new iam.ServicePrincipal('apigateway.amazonaws.com')
    })
    props.dataStream.grantWrite(this.role)

    /*
    * API Gateway設定
    -------------------------------------------------------------------------- */
    // DeployOptions
    if (props.enableDetailMonitoring) {
      // 詳細モニタリング有効の場合
      this.stageOption = this.createStageOptionForDetailedMonitoring()
    } else {
      // 詳細モニタリング無効の場合
      this.stageOption = this.createStageOption()
    }

    // Rest API
    if (props.type === apigw.EndpointType.PRIVATE) {
      // Private APIの場合
      if (props.vpc === undefined) throw new Error('For private apigw, you must specify a VPC.')
      this.vpcEndpoint = this.createVPCEndpoint(props.vpc)
      this.restApi = this.createPrivateRestApi()
    } else {
      // Public APIの場合
      this.restApi = this.createRegionalRestApi()
    }

    // リソース(パス)
    const streamsResource = this.restApi.root.addResource('streams')
    const recordsResource = streamsResource.addResource('records')

    // AWS統合設定: KDS PutRecords API
    const putRecordsIntegrationOptions = this.createPutRecordsIntegrationOption(props.dataStream)
    const putRecordsAwsIntegration = new apigw.AwsIntegration({
      service: 'kinesis',
      action: 'PutRecords',
      options: putRecordsIntegrationOptions
    })

    // AWS統合設定: KDS PutRecord API
    const putRecordIntegrationOptions = this.createPutRecordIntegrationOption(props.dataStream)
    const putRecordAwsIntegration = new apigw.AwsIntegration({
      service: 'kinesis',
      action: 'PutRecord',
      options: putRecordIntegrationOptions
    })

    // メソッドリクエスト, レスポンス
    const methodOptions: apigw.MethodOptions = {
      methodResponses: [
        {
          statusCode: '200'
        },
        {
          statusCode: '500'
        }
      ]
    }
    // メソッド追加
    recordsResource.addMethod('PUT', putRecordAwsIntegration, methodOptions)
    recordsResource.addMethod('PUT', putRecordsAwsIntegration, methodOptions)
  }

  /**
   * 詳細モニタリングする場合のステージ設定を作成
   */
  createStageOptionForDetailedMonitoring(): apigw.StageOptions {
    // CloudWatch LogGroup アクセスログ用
    // 実行ログはLogGroupの指定不可
    const accessLogGroup = new logs.LogGroup(this, 'LambdaLogGroup', {
      logGroupName: `/aws/apigw/accesslog/${Stack.of(this).stackName}`,
      removalPolicy: RemovalPolicy.DESTROY,
      retention: logs.RetentionDays.ONE_DAY
    })

    return {
      stageName: 'v1',
      tracingEnabled: true, // X-Ray
      dataTraceEnabled: true, // 実行ログ
      loggingLevel: apigw.MethodLoggingLevel.INFO, // 実行ログ出力レベル
      accessLogDestination: new apigw.LogGroupLogDestination(accessLogGroup), // アクセスログ出力先
      accessLogFormat: apigw.AccessLogFormat.clf(), // アクセスログフォーマット
      metricsEnabled: true // 詳細メトリクス
    }
  }

  /**
   * シンプルなステージ設定を作成
   */
  createStageOption(): apigw.StageOptions {
    return {
      stageName: 'v1',
      tracingEnabled: false,
      dataTraceEnabled: false,
      metricsEnabled: false
    }
  }

  /**
   * Private APIのためのVPCエンドポイント作成
   * @param vpc_ エンドポイントを作成するVPC
   */
  createVPCEndpoint(vpc_: ec2.Vpc): ec2.InterfaceVpcEndpoint {
    // SG
    const vpcEndpointSecurityGroup = new ec2.SecurityGroup(this, 'VpcEndpointSG', {
      vpc: vpc_
    })
    vpcEndpointSecurityGroup.addIngressRule(ec2.Peer.ipv4(vpc_.vpcCidrBlock), ec2.Port.allTcp())

    // VPC Endpoint
    return new ec2.InterfaceVpcEndpoint(this, 'VpcEndpoint', {
      vpc: vpc_,
      service: ec2.InterfaceVpcEndpointAwsService.APIGATEWAY,
      subnets: { subnets: vpc_.publicSubnets },
      securityGroups: [vpcEndpointSecurityGroup],
      open: false
    })
  }

  /**
   * PrivateタイプのRest APIを作成
   */
  createPrivateRestApi(): apigw.RestApi {
    return new apigw.RestApi(this, 'Default', {
      deployOptions: this.stageOption,
      endpointConfiguration: {
        types: [apigw.EndpointType.PRIVATE],
        vpcEndpoints: [this.vpcEndpoint]
      },
      policy: new iam.PolicyDocument({
        statements: [
          new iam.PolicyStatement({
            effect: iam.Effect.DENY,
            principals: [new iam.AnyPrincipal()],
            actions: ['execute-api:Invoke'],
            resources: ['execute-api:/*'],
            conditions: {
              StringNotEquals: { 'aws:sourceVpce': this.vpcEndpoint.vpcEndpointId }
            }
          }),
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            principals: [new iam.AnyPrincipal()],
            actions: ['execute-api:Invoke'],
            resources: ['execute-api:/*']
          })
        ]
      })
    })
  }

  /**
   * RegionalタイプのRest APIを作成
   */
  createRegionalRestApi(): apigw.RestApi {
    return new apigw.RestApi(this, 'Default', {
      deployOptions: this.stageOption,
      endpointConfiguration: {
        types: [apigw.EndpointType.REGIONAL]
      }
    })
  }

  /**
   * KDS PutRecords API統合用の設定作成
   * @param dataStream
   */
  createPutRecordsIntegrationOption(dataStream: Stream): apigw.IntegrationOptions {
    return {
      requestParameters: {
        'integration.request.header.Content-Type': "'x-amz-json-1.1'"
      },
      credentialsRole: this.role,
      passthroughBehavior: apigw.PassthroughBehavior.WHEN_NO_TEMPLATES,
      requestTemplates: {
        'application/json': `
{
"StreamName": "${dataStream.streamName}",
"Records": [
#foreach($elem in $input.path('$.records'))
  {
    "Data": "$util.base64Encode($elem.data)",
    "PartitionKey": "$elem.PartitionKey"
  }#if($foreach.hasNext),#end
  #end
]
}`
      },
      // 統合レスポンスの設定
      integrationResponses: [
        {
          statusCode: '200',
          selectionPattern: '200',
          responseTemplates: {
            'application/json': `
{
"Code": "200",
"Message": "OK",
"FailedRecordCount": "$input.path('$.FailedRecordCount')"
}
#if($input.path('$.FailedRecordCount') != '0')
#set($context.responseOverride.status) = 500
#end`
          }
        },
        {
          statusCode: '500',
          selectionPattern: '5d{2}',
          responseTemplates: {
            'application/json': `
{
"Code": "500",
"Message": "ServerError",
"FailedRecordCount": "$input.path('$.FailedRecordCount')"
}`
          }
        }
      ]
    }
  }

  /**
   * KDS PutRecord API統合用のオプション作成
   * @param dataStream
   */
  createPutRecordIntegrationOption(dataStream: Stream): apigw.IntegrationOptions {
    return {
      requestParameters: {
        'integration.request.header.Content-Type': "'x-amz-json-1.1'"
      },
      credentialsRole: this.role,
      passthroughBehavior: apigw.PassthroughBehavior.WHEN_NO_TEMPLATES,
      requestTemplates: {
        'application/json': `
{
"StreamName": "${dataStream.streamName}",
"Data": "$util.base64Encode($input.path('$.data'))",
"PartitionKey": "$input.path('$.PartitionKey')"
}`
      },
      // 統合レスポンスの設定
      integrationResponses: [
        {
          statusCode: '200',
          selectionPattern: '200',
          responseTemplates: {
            'application/json': `
{
"Code": "200",
"Message": "OK",
"SequenceNumber": "$input.path('$.sequnceNumber')",
"shardId": "$input.path('$.shardId')",
}`
          }
        },
        {
          statusCode: '500',
          selectionPattern: '5d{2}',
          responseTemplates: {
            'application/json': `
{
#set($inputRoot = $input.path('$'))
"Code": "500",
"Message": "$inputRoot.errorMessage",
"ErrorType": "$inputRoot.errorType",
"StackTrace": "$inputRoot.stackTrace"
}`
          }
        }
      ]
    }
  }
}