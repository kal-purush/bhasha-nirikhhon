import { BadRequestException, Body, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { CoursePositionEntity } from '../entity/coursePosition.entity';
import { Repository } from 'typeorm';
import { CoursePositionDto } from '../dto/coursePosition.dto';
import { plainToInstance } from 'class-transformer';
import { SchedulerTemplateEntity } from '../../schedulerTemplate/entity/schedulerTemplate.entity';
import { CoursesEntity } from '../../courses/entity/courses.entity';

@Injectable()
export class CoursePositionService {
  constructor(
    @InjectRepository(CoursePositionEntity)
    private readonly coursePositionRepository: Repository<CoursePositionEntity>,
  ) {}
  private checkNull(coursePositionDto: CoursePositionDto) {
    if (coursePositionDto.days === null || coursePositionDto.periods === null) {
      throw new BadRequestException('Days and periods are required.');
    }
  }
  async createCoursePosition(coursePositionDto: CoursePositionDto) {
    // check null
    this.checkNull(coursePositionDto);
    // check exists
    // create new course position
    const newCoursePosition = await this.coursePositionRepository
      .createQueryBuilder()
      .insert()
      .into(CoursePositionEntity)
      .values({
        days: coursePositionDto.days,
        periods: coursePositionDto.periods,
      })
      .execute();
    return {
      message: 'Create new course position successfully!',
      newCoursePosition: newCoursePosition,
    };
  }
}