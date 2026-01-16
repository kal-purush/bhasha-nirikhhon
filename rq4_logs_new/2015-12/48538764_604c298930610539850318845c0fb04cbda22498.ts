import Subscription from "./Subscription";
import Stream from "./Stream";

export default class MultiplexerSubscription extends Subscription {

    private subscriptions: Subscription[];

    constructor(subscription: Subscription[]) {
        super();
        this.subscriptions = subscription;
    }

    isOpen(): boolean {
        let open = true;
        this.subscriptions.forEach(sub => {
            if(!sub.isOpen()) {
                open = false;
            }
        });
        return open;
    }

    unsubscribe(): void {
        this.subscriptions.forEach(subscription => {
            subscription.unsubscribe();
        });
    }
}