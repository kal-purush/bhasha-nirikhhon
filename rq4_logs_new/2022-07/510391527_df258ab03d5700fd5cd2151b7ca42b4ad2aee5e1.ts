import { ReactionAddedEvent } from '@slack/bolt';
import { WebClient } from '@slack/web-api';
import IAddReactionUseCase from '../usecases/IAddReactionUseCase';
import AddMeowReactionUseCase from '../usecases/AddMeowReactionUseCase';

export default class AddReactionController {
    private readonly event: ReactionAddedEvent;
    private readonly useCase: IAddReactionUseCase;
    private readonly client: WebClient;

    constructor(event: ReactionAddedEvent, client: WebClient) {
        this.client = client;

        if (event.item.type !== 'message') {
            throw new Error(`${event.item.type}は対象外のリアクションです。`);
        }

        switch (event.reaction) {
            case 'meow_mudamudamuda':
                this.useCase = new AddMeowReactionUseCase(this.client, event.item.channel, event.item.ts);
                break;
            default:
                throw new Error(`${event.reaction}は対象外のリアクションです。`);
        }
        this.event = event;
    }

    public async execute(): Promise<void> {
        await this.useCase.execute();
    }
}