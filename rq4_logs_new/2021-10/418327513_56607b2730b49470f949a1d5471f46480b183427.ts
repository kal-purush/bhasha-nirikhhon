import {AWSError, SQS} from "aws-sdk";
import {SendMessageResult} from "aws-sdk/clients/sqs";

export interface MetaConfig{
    apiVersion?: string
    region: string,
}

export interface QueueConfig {
    queueUrl: string,
    queueName: string
}


export abstract class BaseQueueClient{
    protected static readonly API_VERSION = '2012-11-05'
    protected queueConfig: QueueConfig;
    protected client: SQS;

    protected constructor(queueConfig: QueueConfig,
                          metaConfig: MetaConfig) {
        this.queueConfig = queueConfig;
        this.client = new SQS({
            apiVersion: metaConfig.apiVersion? metaConfig.apiVersion : BaseQueueClient.API_VERSION,
            region: metaConfig.region
        });
    }
}



export interface FIFOPublisher{
    sendMessage(messageBody: string, messageDeduplicationId: string, messageGroupId: string): any
}

export interface StandardPublisher{
    sendMessage(messageBody: string): any
}