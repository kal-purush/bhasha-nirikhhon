import { PostProfileResponse } from '@app/contents/post/dto/post-profile.response';
import { TopicProfileResponse } from '@app/contents/topic/dto/topic-profile.response';
import { VideoProfileResponse } from '@app/contents/video/dto/video.profile.response';

export enum ContentsType {
  VIDEO = 'VIDEO',
  POST = 'POST',
  TOPIC = 'TOPIC',
}
export type PostsResponse = PostProfileResponse | VideoProfileResponse;

export type ContentsResponse =
  | PostProfileResponse
  | VideoProfileResponse
  | TopicProfileResponse;