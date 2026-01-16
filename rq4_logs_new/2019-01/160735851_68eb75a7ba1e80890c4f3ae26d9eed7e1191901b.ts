import { ApiModelProperty } from '@nestjs/swagger';
import {IsString, IsNotEmpty, IsInt} from 'class-validator';

export class CreateMap {

  /** Name. */
  @ApiModelProperty()
  @IsNotEmpty()
  @IsString() readonly name: string;

  /** Height. */
  @ApiModelProperty()
  @IsNotEmpty()
  @IsInt() readonly height: number;

  /** Width. */
  @ApiModelProperty()
  @IsNotEmpty()
  @IsInt() readonly width: number;

  /** Content. */
  @ApiModelProperty()
  @IsNotEmpty()
  @IsString() readonly content: string[];

}