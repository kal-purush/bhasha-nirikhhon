module mpp {

    // https://developers.google.com/youtube/js_api_reference
    export enum PlayerState {
        unstarted = -1,
        ended = 0,
        playing = 1,
        paused = 2,
        buffering = 3,
        video_cued = 5
    }

    export interface Player {
        playVideo(): void;
        pauseVideo(): void;
        stopVideo(): void;
        getPlayerState(): PlayerState;
        getDuration(): number;
        getVideoUrl(): String;
        getCurrentTime(): number;
        seekTo(seconds: number, allowSeekAhead: boolean): void;
    }

    export interface Session {
        name: string;
        videoUrl: string;
        markers: number[];
        isPlaceholder?: boolean;
    }

    var mppApp = angular.module('mppApp', [
        'youtube-embed',
        'ui.keypress',
        'ui.event',
        'LocalStorageModule',
    ]).config((localStorageServiceProvider) => {
        localStorageServiceProvider.setPrefix('mpp');
    });

    service.session.register(mppApp);
    filter.register(mppApp);
    controller.main.register(mppApp);
    dropdown.register(mppApp);
}


