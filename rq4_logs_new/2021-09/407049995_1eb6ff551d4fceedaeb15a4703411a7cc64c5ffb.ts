import { ApiProperty } from "@nestjs/swagger";
import { IsNumberString } from "class-validator";

export class ContentToPlaylistDto {
  @IsNumberString()
  @ApiProperty({
    description: "This is content's id to attach",
    example: "1",
    type: String,
  })
  contentID: string;

  @IsNumberString()
  @ApiProperty({
    description: "This is content's order in playlist",
    example: "2",
    type: String,
  })
  order: string;

  @IsNumberString()
  @ApiProperty({
    description: "This is content's duration",
    example: "5.67",
    type: String,
  })
  duration: string;
}