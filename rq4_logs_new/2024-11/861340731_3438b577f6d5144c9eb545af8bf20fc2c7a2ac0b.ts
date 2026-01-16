import { Injectable, BadRequestException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { CourseValueEntity } from './courseValue.entity';
import { CourseValueDto } from './courseValue.dto';
import { plainToInstance } from 'class-transformer';
@Injectable()
export class CourseValueService {
  constructor(
    @InjectRepository(CourseValueEntity)
    private readonly courseValueRepository: Repository<CourseValueEntity>,
  ) {}

  async createCourseValue(courseValueDto: CourseValueDto) {
    try {
      const newCourseValue = plainToInstance(CourseValueEntity, courseValueDto);

      await this.courseValueRepository.save(newCourseValue);

      return {
        message: 'Course Value created successfully',
        courseValue: newCourseValue,
      };
    } catch (error) {
      console.log(error);
      throw new BadRequestException('Error creating Course Value');
    }
  }
}