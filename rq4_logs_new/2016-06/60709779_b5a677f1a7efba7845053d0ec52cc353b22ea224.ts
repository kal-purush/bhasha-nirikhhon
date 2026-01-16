import { EventListener, EventSubscriber } from 'bap-node-microframework/decorators';
import { BaseEventSubscriber } from 'bap-node-microframework/core';

@EventSubscriber()
export class FriendEvents extends BaseEventSubscriber {

    @EventListener('test')
    friendPost(data, dispatcher) {
        console.log("Friend " + data + " added");
        dispatcher.emit('testMessage');
    }

    @EventListener('testMessage')
    testMessage() {
        console.log("Test message");
    }
}