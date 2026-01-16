import AddReactionUsecase from '../../usecases/AddReactionUsecase';
import { ReactionAddedEvent } from '@slack/bolt';
import { WebClient } from '@slack/web-api';

describe('正しくパターンごとに振り分けられるかのテスト', () => {
    test('存在するTypeとファイル名で、meowのとき成功する', () => {
        const event: ReactionAddedEvent = {
            type: 'reaction_added',
            user: 'testUser',
            reaction: 'moriage_meow',
            item_user: 'testUser',
            item: {
                type: 'message',
                channel: 'testChannel',
                ts: 'timestamp'
            },
            event_ts: 'event_ts'
        };

        const client = new WebClient();
        new AddReactionUsecase(event, client);

        expect(true).toBe(true);
    });

    test('prefixがmoriageじゃないリアクションがされたときエラー', () => {
        const event: ReactionAddedEvent = {
            type: 'reaction_added',
            user: 'testUser',
            reaction: 'morige_meow',
            item_user: 'testUser',
            item: {
                type: 'message',
                channel: 'testChannel',
                ts: 'timestamp'
            },
            event_ts: 'event_ts'
        };

        const client = new WebClient();
        expect(() => new AddReactionUsecase(event, client)).toThrowError();
    });

    test('prefixがmoriageで、praiseのリアクションがされたとき成功', () => {
        const event: ReactionAddedEvent = {
            type: 'reaction_added',
            user: 'testUser',
            reaction: 'moriage_praise',
            item_user: 'testUser',
            item: {
                type: 'message',
                channel: 'testChannel',
                ts: 'timestamp'
            },
            event_ts: 'event_ts'
        };

        const client = new WebClient();
        new AddReactionUsecase(event, client);

        expect(true).toBe(true);
    });
});