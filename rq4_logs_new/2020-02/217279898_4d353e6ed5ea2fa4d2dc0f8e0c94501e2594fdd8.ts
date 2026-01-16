declare namespace KalturaPlayerTypes {

  export interface KalturaPlayerManager {
    setup: (KalturaPlayerConfig) => Player;
  }

  export interface KalturaPlayerConfig {
    targetId: string,
    playback: PlaybackConfig,
    provider: ProviderOptionsObject,
  }

  export interface PlaybackConfig {
    autoplay: boolean
  }

  export interface ProviderOptionsObject {
    partnerId: string,
    ks: string,
    uiConfId: string
  }

  export interface MediaInfoObject {
    entryId: string,
    ks?: string
  }

  export interface Player {
    loadMedia(mediaInfo: MediaInfoObject): Promise;
    destroy(): void;
  }
}