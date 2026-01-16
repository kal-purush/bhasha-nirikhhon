import {
  BasicPost,
  MeetingPost,
  NoticePost,
  PeerReviewPost,
  VotePost,
} from '~/shared/types';

export type PostRequestType =
  | BasicPostRequest
  | NoticePostRequest
  | VotePostRequest
  | PeerReviewPostRequest
  | MeetingPostRequest;

export type BasicPostRequest = BasicPost & { title: string; projectId: number };

export type NoticePostRequest = NoticePost & {
  title: string;
  projectId: number;
};

export type VotePostRequest = Omit<
  VotePost,
  'voteId' | 'createAt' | 'createBy'
> & { projectId: number };

export type MeetingPostRequest = Omit<
  MeetingPost,
  'joinYn' | 'createAt' | 'createBy'
> & { projectId: number };

export type PeerReviewPostRequest = Omit<
  PeerReviewPost,
  'joinYn' | 'createAt' | 'createBy' | 'reviewComments'
> & { projectId: number };