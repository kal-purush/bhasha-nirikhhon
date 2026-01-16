interface UserProps {
    name: string;
    age: number;
}

interface UserEvent {
    type: string;
    data: UserProps;
}

type Callback = (event: UserEvent) => void;
type Events = { [key: string]: Callback[] };

interface IUser {
    events: Events;
    get(prop: string): number | string;
    set(update: Partial<UserProps>): void;
    on(eventName: string, callback: Callback): void;
    trigger(eventName: string);
}

export class User implements IUser {
    events = {};

    constructor(private data: UserProps) {}

    get(prop: string): number | string {
        return this.data[prop];
    }

    set(update: Partial<UserProps>): void {
        Object.assign(this.data, update);
    }

    on(eventName: string, callback: Callback) {
        if (!this.events[eventName]) {
            this.events[eventName] = [];
        }

        this.events[eventName].push(callback);
    }

    trigger(eventName: string) {
        const handlers = this.events[eventName];
        if (!handlers || handlers.length === 0) {
            console.warn(
                new Error(`${eventName} event is not assigned to any user`)
            );

            return;
        }

        this.events[eventName].forEach(callback => {
            callback({
                type: eventName,
                data: this.data,
            });
        });
    }
}