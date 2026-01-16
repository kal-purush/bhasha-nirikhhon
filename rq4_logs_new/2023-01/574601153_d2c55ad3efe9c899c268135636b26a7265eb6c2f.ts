import { ApiProperty } from '@nestjs/swagger';
import { IsDate, IsNotEmpty, IsString } from 'class-validator';

import { PostProfileResponse } from '@app/contents/post/dto/post-profile.response';
import { UserBookmarkCommand } from '@app/user/user.commands';

export class UserBookmarkResponse {
  @ApiProperty({
    description: '북마크 아이디',
    example: 'a6f9cd0d-8cae-414e-bb68-ae9b6d83118d',
  })
  @IsString()
  @IsNotEmpty()
  id: string;

  @ApiProperty({
    description: '유저 아이디',
    example: '3ca549ce-a167-4f66-b6d6-27a6745e4839',
  })
  @IsString()
  @IsNotEmpty()
  userId: string;

  @ApiProperty({
    description: '게시글 아이디',
    example: '7eb3903c-7259-403a-9b0e-43771f58a231',
  })
  @IsString()
  @IsNotEmpty()
  postId: string;

  @ApiProperty({
    description: '게시글 정보',
    type: PostProfileResponse,
  })
  post: PostProfileResponse;

  @ApiProperty({
    description: '북마크의 마지막 추가/수정일',
    example: '2021-08-01T00:00:00.000Z',
  })
  @IsDate()
  @IsNotEmpty()
  updatedAt: Date;

  constructor(userBookmark: UserBookmarkCommand) {
    this.id = userBookmark.id;
    this.userId = userBookmark.user.id;
    this.postId = userBookmark.post.id;
    this.post = new PostProfileResponse(userBookmark.post);
  }
}