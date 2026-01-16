export interface ClientDisconnect {
    clientId: string;
    reason: "DISCONNECT" | "NOT_CONNECTED";
}

export interface UsageStats {
    numberOfConnectedClients: number;
}

export interface SubscribeDisconnect {
    type: "SUBSCRIBE_DISCONNECT";
    clientId: string;
}
export interface UnsubscribeDisconnect {
    type: "UNSUBSCRIBE_DISCONNECT";
    clientId: string;
}

export interface SubscribeUsageStats {
    type: "SUBSCRIBE_USAGE_STATS";
}
export interface UnsubscribeUsageStats {
    type: "UNSUBSCRIBE_USAGE_STATS";
}

export type ControlMessage = ClientDisconnect | UsageStats;
export type ControlSubscribe = SubscribeDisconnect | SubscribeUsageStats;
export type ControlUnsubscribe = UnsubscribeDisconnect | UnsubscribeUsageStats;

export interface ControlMessageSubscribeMap {
    CONTROL: ControlMessage;
}

export interface ControlMessagePublishMap {
    CONTROL_SUBSCRIBE: ControlSubscribe;
    CONTROL_UNSUBSCRIBE: ControlUnsubscribe;
}