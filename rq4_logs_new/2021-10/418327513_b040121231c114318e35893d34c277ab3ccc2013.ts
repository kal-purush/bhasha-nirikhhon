import {BaseQueueClient, MetaConfig, QueueConfig} from "../core/infra/BaseQueueClient";
import {IPoller} from "../core/infra/QueuePoller";
import {Consumer, SQSMessage} from "sqs-consumer"


export abstract class Ordering<MessageProps> extends BaseQueueClient implements IPoller<MessageProps> {
    protected poller?: Consumer;

    protected constructor(config: QueueConfig & MetaConfig) {
        super({...config}, {...config});
        this.poller = Consumer.create({
            sqs: this.client,
            handleMessage: async (message: SQSMessage) => this.onMessageHandled(JSON.parse(message.Body) as MessageProps, message)
        });
        this.onEventReady();
    }

    private onEventReady(){
        this.poller.on('message_received', (message) => this.onMessageReceived(JSON.parse(message.Body) as MessageProps, message));
        this.poller.on('message_processed', (message) => this.onMessageProcessed(JSON.parse(message.Body) as MessageProps, message));
        this.poller.on('error', (error) => this.onError(error));
    }

    abstract onError(err);

    abstract onMessageHandled(messageBody: MessageProps, sqsMessage: any);

    abstract onMessageProcessed(messageBody: MessageProps, sqsMessage: any);

    abstract onMessageReceived(messageBody: MessageProps, sqsMessage: any);

    protected start(){
        this.poller.start();
    };


}