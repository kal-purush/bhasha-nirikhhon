import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Report } from './report.schema';
import { UserApp } from 'src/constants/user';
import { VideoRepository } from 'src/video/video.repository';
import { ReportNSWF } from 'src/constants/report';

@Injectable()
export class ReportRepository {
  constructor(
    @InjectModel(Report.name) private reportModel: Model<Report>,
    private videoRepository: VideoRepository,
  ) {}

  async createReport(
    user: UserApp,
    videoId: string,
    type: string,
    content: string,
  ) {
    const newReport = await this.reportModel.create({
      user,
      videoId,
      type,
      content,
    });
    return newReport;
  }

  async getReportsByVideoId(videoId: string) {
    const reports = await this.reportModel.find({ videoId });
    return reports;
  }

  async getReportById(reportId: string) {
    const report = await this.reportModel.findById(reportId);
    if (!report) throw new NotFoundException(`ReportId ${reportId} not found`);
    return report;
  }

  async acceptReport(reportId: string, videoId: string, type: string) {
    const report = await this.getReportById(reportId);
    const convertType: string = ReportNSWF[type];
    await this.videoRepository.updateVideoNSFW(videoId, true, convertType);
    const updateReport = await this.reportModel.findByIdAndUpdate(
      report._id,
      {
        $set: { approved: true },
      },
      {
        new: true,
        runValidators: true,
      },
    );
    return updateReport;
  }
}