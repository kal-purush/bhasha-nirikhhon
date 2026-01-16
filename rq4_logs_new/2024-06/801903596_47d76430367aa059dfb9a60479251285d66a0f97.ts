import { IsBoolean, IsEnum, IsNumber, IsOptional } from 'class-validator';
import { PageOptionsDto } from './pageOptions.dto';
import { FeedbackSortField } from '../../constants';

export class FeedbackPageOptions extends PageOptionsDto {
  @IsEnum(FeedbackSortField, {
    message: 'error.enumNotMatch.feedbackSortField',
  })
  sortField: FeedbackSortField = FeedbackSortField.CREATED_AT;

  @IsNumber()
  productId: number;

  @IsOptional()
  @IsNumber()
  star: number;

  @IsOptional()
  @IsBoolean()
  haveImage: boolean = false;

  @IsOptional()
  @IsBoolean()
  haveContent: boolean = false;
}