import { http } from '@/api';
import { FollowPostResponse, SubscriptionListResponse } from './types';

export const getFollowList = (page: number) =>
  http.get<SubscriptionListResponse>({
    url: '/blogs/subscriptions/follows',
    params: { page },
  });

export const getFollowerList = (page: number) =>
  http.get<SubscriptionListResponse>({
    url: '/blogs/subscriptions/followers',
    params: { page },
  });

export const getFollowPost = (page: number) =>
  http.get<FollowPostResponse>({
    url: '/blogs/subscriptions/follows/posts',
    params: { page },
  });