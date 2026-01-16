import { ApiProperty } from '@nestjs/swagger';
import { Expose } from 'class-transformer';
import { Event, EventStatus } from 'src/modules/events/schemas/event.schema';
import toDTO from 'src/utils/dtoConvertor';
import { TransformObjectId } from 'src/utils/transformers';

export class ResponseEventNoTravelsDTO {
  public static build(event: Event): ResponseEventNoTravelsDTO {
    return toDTO(ResponseEventNoTravelsDTO, event);
  }

  @Expose()
  @ApiProperty()
  @TransformObjectId()
  public _id: string;

  @Expose()
  @ApiProperty()
  public name: string;

  @Expose()
  @ApiProperty()
  public start_date: Date;

  @Expose()
  @ApiProperty()
  public end_date: Date;

  @Expose()
  @ApiProperty({ required: false })
  public location?: string;

  @Expose()
  @ApiProperty({ enum: Object.values(EventStatus) })
  public status: string;
}