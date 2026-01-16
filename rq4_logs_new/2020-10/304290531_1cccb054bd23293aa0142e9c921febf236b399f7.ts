import logger from '@/utils/logger';
import { isFavoriteRequest, isLikeRequest } from './requestTypeCheck';

export const requestFilter: chrome.webRequest.RequestFilter = {
  urls: ['https://api.bilibili.com/*'],
  types: ['xmlhttprequest'],
};

enum RequestType {
  Like,
  Favorite,
}

// 记录网络请求的 id 及其类型
const requests = new Map<string, RequestType>();

// 在发起请求时记录请求类型（收藏、点赞等）
export function beforeRequestHandler(details: chrome.webRequest.WebRequestBodyDetails) {
  logger.info(details);
  if (isLikeRequest(details)) {
    requests.set(details.requestId, RequestType.Like);
  } else if (isFavoriteRequest(details)) {
    requests.set(details.requestId, RequestType.Favorite);
  }
}

// 请求成功结束，清除记录
export function requestCompletedHandler(details: chrome.webRequest.WebResponseCacheDetails) {
  logger.info(details);
  logger.info(details.requestId, requests.get(details.requestId));
  requests.delete(details.requestId);
}

// 请求失败，清除记录
export function requestErrorhandler(details: chrome.webRequest.WebResponseErrorDetails) {
  requests.delete(details.requestId);
}