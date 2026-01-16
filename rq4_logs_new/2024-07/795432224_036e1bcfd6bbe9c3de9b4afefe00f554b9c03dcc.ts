import * as path from 'path'

import { Construct } from 'constructs'
import { Duration, RemovalPolicy, Stack } from 'aws-cdk-lib'
import * as logs from 'aws-cdk-lib/aws-logs'
import * as nodejsLambda from 'aws-cdk-lib/aws-lambda-nodejs'
import * as lambda_ from 'aws-cdk-lib/aws-lambda'
import { SqsDlq } from 'aws-cdk-lib/aws-lambda-event-sources'
import * as sqs from 'aws-cdk-lib/aws-sqs'
import * as iam from 'aws-cdk-lib/aws-iam'
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb'
import { type Stream } from 'aws-cdk-lib/aws-kinesis'

interface KdsLambdaConsumerProps {
  prefix: string
  dataStream: Stream
  /** Lambda関数のhandlerモジュールファイルパス */
  lambdaEntry: string
  /** Lambda Event Source Mappiing設定  */
  eventSourceMappingOption?: lambda_.EventSourceMappingOptions
  /** DynamoDBの読み込み書き込みキャパシティ設定 */
  billing?: dynamodb.Billing
}

/**
 * Kinesis Lambda Consumer
 * DynamoDBによる重複排除処理を含む
 */
export class KdsLambdaConsumer extends Construct {
  public readonly kdsConsumerFunction: lambda_.Function

  constructor(scope: Construct, id: string, props: KdsLambdaConsumerProps) {
    super(scope, id)

    props.billing ??= dynamodb.Billing.provisioned({
      readCapacity: dynamodb.Capacity.fixed(3),
      writeCapacity: dynamodb.Capacity.autoscaled({ maxCapacity: 5 })
    })

    /*
    * DynamoDB
    -------------------------------------------------------------------------- */
    const table = new dynamodb.TableV2(this, 'Table', {
      partitionKey: { name: 'recordId', type: dynamodb.AttributeType.STRING },
      billing: props.billing,
      tableClass: dynamodb.TableClass.STANDARD,
      encryption: dynamodb.TableEncryptionV2.dynamoOwnedKey(),
      pointInTimeRecovery: false,
      removalPolicy: RemovalPolicy.DESTROY,
      contributorInsights: true,
      timeToLiveAttribute: 'expired',
      dynamoStream: dynamodb.StreamViewType.NEW_IMAGE,
      tags: [{ key: 'Name', value: `${props.prefix}-lambda-consumer-deduplication-table` }]
    })

    /*
    * Lambda
    -------------------------------------------------------------------------- */
    // DLQ
    const kdsDlq = new sqs.Queue(this, 'KinesisDeadLetterQueue')

    // Lambda Layer
    const powertoolsLayer = lambda_.LayerVersion.fromLayerVersionArn(
      this,
      'PowertoolsLayer',
      `arn:aws:lambda:${Stack.of(this).region}:094274105915:layer:AWSLambdaPowertoolsTypscriptv2:6`
    )
    const customlayer = new lambda_.LayerVersion(this, 'CustomLayer', {
      removalPolicy: RemovalPolicy.DESTROY,
      code: lambda_.Code.fromAsset(path.join('resources', 'lambda_layer', 'kinesis')),
      compatibleArchitectures: [lambda_.Architecture.X86_64, lambda_.Architecture.ARM_64]
    })

    // Lambda Function
    const funcName = `${props.prefix}-kds-consumer-func`
    this.kdsConsumerFunction = new nodejsLambda.NodejsFunction(this, 'LambdaFunc', {
      functionName: funcName,
      entry: props.lambdaEntry,
      handler: 'handler',
      runtime: lambda_.Runtime.NODEJS_20_X,
      architecture: lambda_.Architecture.ARM_64,
      memorySize: 512,
      timeout: Duration.minutes(3),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ['xray:*'],
          resources: ['*']
        }),
        new iam.PolicyStatement({
          actions: ['cloudwatch:PutMetricStream', 'cloudwatch:PutMetricData'],
          resources: ['*']
        })
      ],
      environment: {
        TABLE_NAME: table.tableName,
        FUNCTION_NAME: funcName,
        METRIC_NAME: 'LambdaBatchSize',
        NAMESPACE: 'Custom/LambdaMetrics'
      },
      layers: [powertoolsLayer, customlayer],
      tracing: lambda_.Tracing.ACTIVE,
      insightsVersion: lambda_.LambdaInsightsVersion.VERSION_1_0_229_0
    })

    // Event Source Mapping
    props.eventSourceMappingOption ??= {
      enabled: true,
      eventSourceArn: props.dataStream.streamArn,
      bisectBatchOnError: true, // 処理エラー時にバッチを２分割
      batchSize: 300, // 関数あたりの処理レコード数
      maxBatchingWindow: Duration.seconds(3), // バッファリングインターバル
      maxRecordAge: Duration.seconds(500), // レコード期限切れまでの時間
      parallelizationFactor: 1, // シャードあたり起動させる関数の数
      reportBatchItemFailures: true, // エラー処理のレポート
      retryAttempts: 5, // リトライ回数
      startingPosition: lambda_.StartingPosition.TRIM_HORIZON,
      onFailure: new SqsDlq(kdsDlq),
      filters: [{ notificationFlag: [true] }]
    }
    const eventSourceMapping = this.kdsConsumerFunction.addEventSourceMapping(
      'EventSourceMapping',
      props.eventSourceMappingOption
    )

    // filter設定
    // L2ではうまく設定されないため、L1のエスケープハッチ利用
    if (props.eventSourceMappingOption.filters !== undefined) {
      // filterのデータ構造をL1のものに変換
      const cfnFilters = props.eventSourceMappingOption.filters.map((value) => {
        return {
          Pattern: JSON.stringify({
            data: value
          })
        }
      })
      const cfnEventSourceMapping = eventSourceMapping.node
        .defaultChild as lambda_.CfnEventSourceMapping
      cfnEventSourceMapping.addPropertyOverride('FilterCriteria', {
        Filters: cfnFilters
      })
    }

    // CloudWatch Logs: LogGroup
    new logs.LogGroup(this, 'LambdaLogGroup', {
      logGroupName: `/aws/lambda/${this.kdsConsumerFunction.functionName}`,
      removalPolicy: RemovalPolicy.DESTROY,
      retention: logs.RetentionDays.ONE_DAY
    })

    // Lambda関数からDynamoDBヘのアクセスを許可する
    table.grantReadWriteData(this.kdsConsumerFunction)
    props.dataStream.grantRead(this.kdsConsumerFunction)
  }
}