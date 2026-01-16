import { EventEmitter2, ListenerFn } from "eventemitter2";
import { RoomState } from "white-web-sdk";

enum BridgeEvent {
    Init = "Init",
    AttributesUpdate = "AttributesUpdate",
    SetAttributes = "SetAttributes",
    MagixEvent = "MagixEvent",
    RegisterMagixEvent = "RegisterMagixEvent",
    RemoveMagixEvent = "RemoveMagixEvent",
    RemoveAllMagixEvent = "RemoveAllMagixEvent",
    RoomStateChanged = "RoomStateChanged",
    NextPage = "NextPage",
    PrevPage = "PrevPage",
}

export enum Events {
    AttributesUpdate = "AttributesUpdate",
    RoomStateChanged = "RoomStateChanged",
}

type BridgeData = {
    kind: string;
    payload: any;
};

type Attributes = {[key: string]: any};

const AllowFollwerEvents: string[] = [BridgeEvent.RegisterMagixEvent, BridgeEvent.RemoveAllMagixEvent];

export class NetlessIframeSDK {
    public targetOrigin: string = "*";
    private emitter: EventEmitter2 = new EventEmitter2();
    private magixEmitter: EventEmitter2 = new EventEmitter2();
    private _attributes: Attributes = {};
    private _roomState: RoomState;

    public constructor(targetOrigin: string, attributes: Attributes, roomState: RoomState) {
        const url = new URL(targetOrigin);
        this.targetOrigin = url.origin;
        this._attributes = attributes;
        this._roomState = roomState;
        window.addEventListener("message", this.messageListener.bind(this));
        window.addEventListener("unload", () => {
            this.emitter.removeAllListeners();
            this.magixEmitter.removeAllListeners();
            this.postMessage(BridgeEvent.RemoveAllMagixEvent, true);
        });
    }

    private messageListener(event: MessageEvent): void {
        if (Object.prototype.toString.call(event.data) !== "[object Object]") {
            return;
        }
        const data: BridgeData = event.data;
        switch (data.kind) {
            case BridgeEvent.AttributesUpdate: {
                this._attributes = Object.assign(this._attributes, data.payload);
                this.emitter.emit(Events.AttributesUpdate, data.payload);
                break;
            }
            case BridgeEvent.MagixEvent: {
                const payload = data.payload;
                this.magixEmitter.emit(payload.event, payload.payload);
                break;
            }
            case BridgeEvent.RoomStateChanged: {
                this._roomState = Object.assign(this._roomState, data.payload);
                this.emitter.emit(Events.RoomStateChanged, data.payload);
                break;
            }
        }
    }

    public get attributes(): Attributes {
        return this._attributes;
    }

    public setAttributes(payload: any): void {
        this._attributes = Object.assign(this._attributes, payload);
        this.postMessage(BridgeEvent.SetAttributes, this.attributes);
    }

    public get roomState(): RoomState {
        return this._roomState;
    }

    public get isFollower(): boolean {
        return this._roomState?.broadcastState?.mode === "follower";
    }

    public get currentIndex(): number {
        return this._roomState.sceneState.index;
    }

    public get currentPage(): number {
        return this.currentIndex + 1;
    }

    private postMessage(kind: string, payload: any): void {
        const isAllowEvent = AllowFollwerEvents.find(event => event === kind);
        if (this.isFollower && !isAllowEvent) {
            return;
        }
        parent.postMessage({ kind, payload }, this.targetOrigin);
    }

    public on(event: string, listener: ListenerFn): void {
        this.emitter.addListener(event, listener);
    }

    public off(event: string, listener: ListenerFn): void  {
        this.emitter.off(event, listener);
    }

    public addMagixEventListener(event: string, listener: ListenerFn): void {
        this.magixEmitter.on(event, listener);
        this.postMessage(BridgeEvent.RegisterMagixEvent, event);
    }

    public dispatchMagixEvent(event: string, payload: any): void {
        this.postMessage(BridgeEvent.MagixEvent, { event, payload });
    }

    public removeMagixEventListener(event: string, listener: ListenerFn): void {
        this.magixEmitter.removeListener(event, listener);
        this.postMessage(BridgeEvent.RemoveMagixEvent, event);
    }

    public nextPage(): void {
        if (this.currentPage >= this.attributes.totalPage) {
            return;
        }
        this.postMessage(BridgeEvent.NextPage, true);
    }

    public prevPage(): void {
        if (this.currentPage <= 1) {
            return;
        }
        this.postMessage(BridgeEvent.PrevPage, true);
    }

    public destroy(): void {
        window.removeEventListener("message", this.messageListener);
        this.emitter.removeAllListeners();
        this.magixEmitter.removeAllListeners();
    }
}