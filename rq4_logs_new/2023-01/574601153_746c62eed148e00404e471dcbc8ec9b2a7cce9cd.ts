import { ForbiddenException, NotFoundException } from '@nestjs/common';

export const REDDIT_CRAWLER_ERRORS = {
  ACCESS_DENIED: 'ACCESS_DENIED',
  POSTS_NOT_FOUND: 'POSTS_NOT_FOUND',
  TIMEOUT: 'TIMEOUT',
};

export class AccessDeniedException extends ForbiddenException {
  constructor() {
    super('사용자 인증에 실패했습니다.', REDDIT_CRAWLER_ERRORS.ACCESS_DENIED);
  }
}

export class PostsNotFoundException extends NotFoundException {
  constructor() {
    super(
      '게시글을 가져오는데 실패하였습니다.',
      REDDIT_CRAWLER_ERRORS.POSTS_NOT_FOUND,
    );
  }
}