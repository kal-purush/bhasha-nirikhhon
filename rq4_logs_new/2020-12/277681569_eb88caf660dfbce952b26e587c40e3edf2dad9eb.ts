import {
  Controller, UseGuards, Get, Query,
} from '@nestjs/common';
import { StreamDataType } from '@truepoint/shared/dist/interfaces/StreamDataType.interface';
import { SearchCalendarStreams } from '@truepoint/shared/dist/dto/stream-analysis/searchCalendarStreams.dto';
import { JwtAuthGuard } from '../../guards/jwt-auth.guard';

// service
import { BroadcastInfoService } from './broadcast-info.service';

// pipe
import { ValidationPipe } from '../../pipes/validation.pipe';

@Controller('broadcast-info')
export class BroadcastInfoController {
  constructor(private readonly broadcastService: BroadcastInfoService) {}

  @Get()
  @UseGuards(JwtAuthGuard)
  getCompleteStreamsList(@Query(new ValidationPipe())
    findDaysStreamRequest: SearchCalendarStreams): Promise<StreamDataType[]> {
    return this.broadcastService.findDayStreamList(
      findDaysStreamRequest.userId,
      findDaysStreamRequest.startDate,
      findDaysStreamRequest.endDate,
    );
  }
}