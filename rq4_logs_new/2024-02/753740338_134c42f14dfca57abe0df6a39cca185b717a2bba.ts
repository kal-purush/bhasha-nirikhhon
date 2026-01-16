import type { GenericMessageToAPI } from "./types";

export function toMilliseconds(sec: number): number {
    return sec * 1000;
}

export function toSeconds(ms: number): number { 
    return ms / 1000;
}

export function secondsToClockString(sec: number): string {
    const seconds = `${sec % 60}`.length === 1 ? `0${sec % 60}` : `${sec % 60}`;
    return `${Math.floor(sec / 60)}:${seconds}`;
}

export function wsSend(ws: WebSocket, message: GenericMessageToAPI) {
    if (ws.readyState !== 1) {
        return;
    }
    console.log(`sending message ${JSON.stringify(message)}`);
    ws.send(JSON.stringify(message));
}