import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { ReportService } from './report.service';
import { GetUser } from 'src/decorators/get-user.decorator';
import { User } from 'src/constants/user';
import { CreateReportDto } from './dto/create-report.dto';
import { ApiBody, ApiOperation, ApiParam, ApiResponse } from '@nestjs/swagger';
import { ReportType } from 'src/constants/report';
import { ReportRes } from './dto/report.res';
import { ReportsRes } from './dto/reports.res';

@Controller('report')
export class ReportController {
  constructor(private reportService: ReportService) {}

  @ApiOperation({ summary: 'Create report' })
  @ApiParam({ name: 'videoId', type: String })
  @ApiBody({
    type: CreateReportDto,
    examples: {
      report_VIOLENCE: {
        value: {
          type: ReportType.VIOLENCE,
          content: 'This video is VIOLENCE',
        },
      },
      report_PORNOGRAPHY: {
        value: {
          type: ReportType.PORNOGRAPHY,
          content: 'This video is porn',
        },
      },
      report_SEXUAL_CONTENT: {
        value: {
          type: ReportType.SEXUAL_CONTENT,
          content: 'This video is sexy charecters',
        },
      },
      report_ADULT_ANIME: {
        value: {
          type: ReportType.ADULT_ANIME,
          content: 'This video is HENTAI',
        },
      },
    },
  })
  @ApiResponse({ type: ReportRes })
  @Post(':videoId')
  async createReport(
    @GetUser() user: User,
    @Param('videoId') videoId: string,
    @Body() createReportDto: CreateReportDto,
  ) {
    const { type, content } = createReportDto;
    return this.reportService.createReport(user, videoId, type, content);
  }
  @Get(':id')
  async getReportsByVideoId(@Param('id') videoId: string) {
    return this.reportService.getReportsByVideoId(videoId);
  }

  @ApiOperation({ summary: 'Get all reports by videoId' })
  @ApiParam({ name: 'videoId', type: String })
  @ApiResponse({ type: ReportsRes })
  @Get('all/:videoId')
  async getAllReportsByVideoId(@Param('videoId') videoId: string) {
    return this.reportService.getReportsByVideoId(videoId);
  }
}