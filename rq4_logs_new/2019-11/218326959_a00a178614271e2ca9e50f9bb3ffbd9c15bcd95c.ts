import { Controller, UseGuards, Post, Delete, Patch, Body, Param } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';
import {
  ApiBearerAuth,
  ApiUnauthorizedResponse,
  ApiInternalServerErrorResponse,
  ApiUseTags,
} from '@nestjs/swagger';

import { ICreateCourseItemData } from '../models/courses.models';
import { SwaggerTags } from '../constants';

@UseGuards(AuthGuard())
@ApiBearerAuth()
@ApiUnauthorizedResponse({ description: 'You have to authorize and provide a token in headers' })
@ApiInternalServerErrorResponse({ description: 'Server internal error' })
@ApiUseTags(SwaggerTags.CourseItems)
@Controller('course-items')
export class CourseItemsController {
  @Post()
  public createCourseItem(@Body() createCourseItemDto: ICreateCourseItemData): any {

  }

  @Delete(':courseItemId')
  public removeCourseItem(@Param('courseItemId') courseItemIdAsString: string): any {
    
  }

  @Patch()
  public modifyCourseItem(@Body() courseItemDto: ICreateCourseItemData): any {

  }
}