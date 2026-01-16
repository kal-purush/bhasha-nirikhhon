import { ApiProperty } from '@nestjs/swagger';
import { Expose } from 'class-transformer';
import { Event } from 'src/modules/events/event.schema';
import { Travels } from 'src/modules/travels/Storage/storage.schema';
import { TravelDTO, TravelsDTO } from 'src/modules/travels/dtos/travels.dto';
import { EventDTO } from './event.dto';

export class ResponseEventDTO extends EventDTO {
  public static build(event: Event, travels: Travels | null): ResponseEventDTO {
    const dto = new ResponseEventDTO();
    dto._id = event.id;
    dto.name = event.name;
    dto.start_date = event.start_date;
    dto.end_date = event.end_date;
    dto.location = event.location;
    dto.travels = travels != null ? travels.travels.map(TravelDTO.build) : undefined;
    return dto;
  }

  @Expose()
  @ApiProperty()
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
  @ApiProperty({ required: false })
  public travels?: TravelsDTO;
}