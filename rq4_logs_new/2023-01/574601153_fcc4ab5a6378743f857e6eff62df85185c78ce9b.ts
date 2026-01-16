import { VideoProfileResponseProperties } from '@app/video/video.comand';
import { Community } from '@domain/post/community.entity';
import { CommunityTitle } from '@domain/post/post';
import { VideoChannel } from '@domain/video/video-channel.entity';

export class VideoProfileResponse implements VideoProfileResponseProperties {
  id: string;
  videoId: string;
  videoUrl: string;
  title: string;
  description: string;
  thumbnailUri: string;
  channel: VideoChannel;
  channelId?: string;
  channelTitle?: string;
  community: Community;
  communityTitle?: CommunityTitle;
  etag: string; // 콘텐츠 업데이트 여부
  uploadedAt: Date;

  constructor(video: VideoProfileResponseProperties) {
    this.id = video.id;
    this.videoId = video.videoId;
    this.videoUrl = 'https://youtu.be/' + video.videoId;
    this.title = video.title;
    this.description = video.description;
    this.thumbnailUri = video.thumbnailUri;
    this.channelId = video.channelId;
    this.channelTitle = video.title;
    this.communityTitle = video.communityTitle;
    this.uploadedAt = video.uploadedAt;
  }
}