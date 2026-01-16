import {SendMessageResult} from "aws-sdk/clients/sqs";

export type PublishedMessageResult = SendMessageResult

export interface FIFOPublisher<MessageType>{
    sendMessage(messageBody: any, messageDeduplicationId: string, messageGroupId: string): PublishedMessageResult | Promise<PublishedMessageResult>
}