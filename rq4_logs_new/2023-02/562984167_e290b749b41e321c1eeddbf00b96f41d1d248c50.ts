import { IsNotEmpty, IsString } from 'class-validator'
import { ApiProperty } from '@nestjs/swagger'

export class EditVocabularyNameDto {
  @IsNotEmpty()
  @IsString()
  @ApiProperty({
    example: 'words from my second book',
    description: 'new name of vocabulary',
  })
  name: string
}