import {
  Injectable,
  BadRequestException,
  ConflictException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { CourseValueEntity } from '../entity/courseValue.entity';
import { CourseValueDto } from '../dto/courseValue.dto';
import { WEEKDAY } from '../utils/course-value.constant';

@Injectable()
export class CourseValueService {
  constructor(
    @InjectRepository(CourseValueEntity)
    private readonly courseValueRepository: Repository<CourseValueEntity>,
  ) {}
  // Check null function
  private checkNull(courseValueDto: CourseValueDto) {
    if (
      courseValueDto.startPeriod === null ||
      courseValueDto.lecture === null ||
      courseValueDto.location === null ||
      courseValueDto.date === null
    ) {
      throw new BadRequestException(
        'Start period, lecture, location and date are required.',
      );
    }
  }
  // check exist function
  private async checkExist(courseValueDto: CourseValueDto) {
    const existingCourseValue = await this.courseValueRepository.findOne({
      where: {
        startPeriod: courseValueDto.startPeriod,
        lecture: courseValueDto.lecture,
        location: courseValueDto.location,
        date: courseValueDto.date,
      },
    });

    if (existingCourseValue) {
      throw new ConflictException('Course value already exists.');
    }
  }
  // check if date valid function
  private checkDateValid(courseValueDto: CourseValueDto) {
    if (!(courseValueDto.date in WEEKDAY)) {
      throw new BadRequestException('Invalid week date');
    }
  }

  async createCourseValue(courseValueDto: CourseValueDto) {
    // check null
    this.checkNull(courseValueDto);
    // check valid week day
    this.checkDateValid(courseValueDto);
    // check exist
    await this.checkExist(courseValueDto);
    // create new course position
    const newCourseValue = await this.courseValueRepository
      .createQueryBuilder()
      .insert()
      .into(CourseValueEntity)
      .values({
        startPeriod: courseValueDto.startPeriod,
        lecture: courseValueDto.lecture,
        location: courseValueDto.location,
        date: courseValueDto.date,
      })
      .execute();
    return {
      message: 'Create new course value successfully!',
      newCourseValue: newCourseValue,
    };
  }
}