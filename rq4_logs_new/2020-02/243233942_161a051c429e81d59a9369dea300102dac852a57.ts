import { PlayInfo } from './play-info';
export interface Callbacks {
    onVolumeChange(value: number): void;
    onError(error: string | object): void;
    onSendMetrics(event: string, info?: object | null): void;
    onPlayStart(stream: string): void;
    onPlayStop(): void;
    onPlayInfo(info: PlayInfo): void;
    onPlayError(level: number, error?: any): void;
    onCameraOn(): void;
    onCameraOff(): void;
    onCameraDenied(): void;
    onPublishStart(): void;
    onPublishStop(): void;
}