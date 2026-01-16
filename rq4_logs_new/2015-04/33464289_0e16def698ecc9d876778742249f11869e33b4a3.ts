declare class Eventable {
    callbacks: {};
    subscribe(event: string, callback: () => any): void;
    fire(event: string): void;
    haveEvent(event: string): boolean;
}
export = Eventable;