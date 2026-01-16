import { NotFoundException } from '@nestjs/common';

export const COMMENT_ERRORS = {
  COMMENT_NOT_FOUND: 'COMMENT_NOT_FOUND',
};

export class CommentNotFoundException extends NotFoundException {
  constructor() {
    super('존재하지 않는 댓글입니다.', COMMENT_ERRORS.COMMENT_NOT_FOUND);
  }
}