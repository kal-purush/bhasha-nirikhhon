import * as request from 'request';
import { Logger } from 'modular-log';
export declare type MessageColors = 'yellow' | 'red' | 'green' | 'purple' | 'gray';
export interface HipChatSettings {
    baseUrl: string;
    room: string | number;
    auth_token: string;
    disableLogger?: boolean;
    defaultNotify?: boolean;
    defaultColor?: MessageColors;
    defaultFormat?: 'text' | 'html';
}
export interface MessageSettings {
    color?: MessageColors;
    notify?: boolean;
    message_format?: 'text' | 'html';
}
export declare class HipChatNotifier {
    private settings;
    log: Logger;
    url: string;
    constructor(settings: HipChatSettings);
    message(message: string, settings?: MessageSettings): request.Request;
    error(message: string, settings?: MessageSettings): request.Request;
    warning(message: string, settings?: MessageSettings): request.Request;
    success(message: string, settings?: MessageSettings): request.Request;
    info(message: string, settings?: MessageSettings): request.Request;
}