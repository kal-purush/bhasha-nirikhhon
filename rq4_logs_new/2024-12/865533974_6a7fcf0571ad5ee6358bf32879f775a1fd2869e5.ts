import { NextFunction, Request, Response } from 'express';
import { IJobService } from '../services/interfaces/IJobsService';
import { CustomError } from '../errors/customErrors';
import { HttpStatusCodes } from '../config/HttpStatusCodes';

export class JobController {
  private jobService: IJobService;

  constructor(jobService: IJobService) {
    this.jobService = jobService;
  }

  async createJob(req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const { userId } = req.params;
      const jobData = req.body;
      console.log(jobData)

      if (!userId) {
        throw new CustomError('User ID is required', HttpStatusCodes.BAD_REQUEST);
      }

      const createdJob = await this.jobService.createJob(userId, jobData);

      res.status(HttpStatusCodes.CREATED).json({
        message: 'Job created successfully',
        data: createdJob,
      });
    } catch (error: any) {
      console.error('Error creating job:', error.message);
      next(
        new CustomError(
          error.message || 'Internal Server Error',
          error.status || HttpStatusCodes.INTERNAL_SERVER_ERROR
        )
      );
    }
  }

//   async fetchJobs(req: Request, res: Response, next: NextFunction): Promise<void> {
//     try {
//       const { userId } = req.params;
//       const { page = 1, limit = 10 } = req.query;

//       if (!userId) {
//         throw new CustomError('User ID is required', HttpStatusCodes.BAD_REQUEST);
//       }

//       const jobs = await this.jobService.fetchJobs(userId, +page, +limit);

//       res.status(HttpStatusCodes.OK).json({
//         message: 'Jobs fetched successfully',
//         data: jobs,
//       });
//     } catch (error: any) {
//       console.error('Error fetching jobs:', error.message);
//       next(
//         new CustomError(
//           error.message || 'Internal Server Error',
//           error.status || HttpStatusCodes.INTERNAL_SERVER_ERROR
//         )
//       );
//     }
//   }

//   async getJobDetails(req: Request, res: Response, next: NextFunction): Promise<void> {
//     try {
//       const { jobId } = req.params;

//       if (!jobId) {
//         throw new CustomError('Job ID is required', HttpStatusCodes.BAD_REQUEST);
//       }

//       const job = await this.jobService.getJobDetails(jobId);

//       if (!job) {
//         throw new CustomError('Job not found', HttpStatusCodes.NOT_FOUND);
//       }

//       res.status(HttpStatusCodes.OK).json({
//         message: 'Job details fetched successfully',
//         data: job,
//       });
//     } catch (error: any) {
//       console.error('Error fetching job details:', error.message);
//       next(
//         new CustomError(
//           error.message || 'Internal Server Error',
//           error.status || HttpStatusCodes.INTERNAL_SERVER_ERROR
//         )
//       );
//     }
//   }

//   async deleteJob(req: Request, res: Response, next: NextFunction): Promise<void> {
//     try {
//       const { jobId } = req.params;

//       if (!jobId) {
//         throw new CustomError('Job ID is required', HttpStatusCodes.BAD_REQUEST);
//       }

//       await this.jobService.deleteJob(jobId);

//       res.status(HttpStatusCodes.OK).json({
//         message: 'Job deleted successfully',
//       });
//     } catch (error: any) {
//       console.error('Error deleting job:', error.message);
//       next(
//         new CustomError(
//           error.message || 'Internal Server Error',
//           error.status || HttpStatusCodes.INTERNAL_SERVER_ERROR
//         )
//       );
//     }
//   }
}