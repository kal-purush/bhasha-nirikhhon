import { ApiProperty, PickType } from "@nestjs/swagger";
import { Exclude, Expose } from "class-transformer";
import { IsString } from "class-validator";

@Exclude()
export class QuestionCategoryDTO {
    @Expose()
    @ApiProperty({ required: false })
    id: string;

    @Expose()
    @ApiProperty({ required: true })
    @IsString()
    category: string;

    @ApiProperty({ required: true })
    @IsString()
    createdBy: string;
  
    @Expose()
    @ApiProperty({ required: false })  
    createDateTime: Date;
  
    @ApiProperty({ required: true })
    @IsString()
    lastChangedBy: string;
}

export class createQuestionCategoryDTO extends PickType(QuestionCategoryDTO, ['category'] as const) {};