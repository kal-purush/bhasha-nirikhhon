import { ApiProperty } from '@nestjs/swagger';

export class SearchDto {
  @ApiProperty({ description: '검색 내용', required: true })
  search: string;
}