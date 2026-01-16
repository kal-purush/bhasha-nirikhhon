export type Callback = <T>(event: T) => void;
export type Events = { [key: string]: Callback[] };

interface IEventing<T> {
    on(eventName: string, callback: Callback): void;
    trigger(data: T, eventName: string);
}

export class Eventing<T> implements IEventing<T> {
    type = {};

    on(eventName: string, callback: Callback) {
        if (!this.type[eventName]) {
            this.type[eventName] = [];
        }

        this.type[eventName].push(callback);
    }

    trigger(data: T, eventName: string) {
        const handlers = this.type[eventName];
        if (!handlers || handlers.length === 0) {
            console.warn(
                new Error(`${eventName} event is not assigned to any user`)
            );

            return;
        }

        this.type[eventName].forEach((callback: Callback) => {
            callback({ type: eventName, data: data });
        });
    }
}