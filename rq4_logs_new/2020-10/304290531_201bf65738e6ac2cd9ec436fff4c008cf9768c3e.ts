import { PauseVideoMessagePayload } from '@/types/message';
import { VideoInfo } from '@/types/video';
import getVideo from './fetchVideo';

/** 视频时长上限/秒 */
const durationLimit = 60 * 5;
/** 视频播放比例下限 */
const playedTimeProportion = 1 / 3;
/** 视频记录个数下限 */
const videoCountLimit = 5;

const videoRecord = new Set<string>();

function shouldRecordVideo({ playedTime, totalDuration }: PauseVideoMessagePayload): boolean {
  return totalDuration <= durationLimit && playedTime >= totalDuration * playedTimeProportion;
}

function shouldRecommendVideo(): boolean {
  return videoRecord.size >= videoCountLimit;
}

let recommendedVideo: VideoInfo | null = null;

export function recordVideoLocally(videoInfo: PauseVideoMessagePayload) {
  if (shouldRecordVideo(videoInfo)) {
    videoRecord.add(videoInfo.bvid);
    // 预拉取视频
    if (videoRecord.size >= videoCountLimit - 1) {
      getVideo().then((video) => {
        recommendedVideo = video;
      });
    }
  }
}

export function getRecommendedVideo(): VideoInfo | null {
  if (!shouldRecommendVideo()) {
    return null;
  }
  const video = recommendedVideo;
  recommendedVideo = null;
  videoRecord.clear();
  return video;
}