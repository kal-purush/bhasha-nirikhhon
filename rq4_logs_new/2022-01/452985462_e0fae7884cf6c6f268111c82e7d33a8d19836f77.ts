import {
  Controller,
  Get,
  Post,
  Patch,
  Delete,
  Body,
  Param,
  HttpStatus,
} from '@nestjs/common';

import { TasksService } from './tasks.service';
import { TasksDTO } from './tasks.dto';

@Controller('tasks')
export class TasksController {
  constructor(private tasksService: TasksService) {}

  @Get()
  async showAllTasks() {
    const tasks =  await this.tasksService.showAll();
    return {
      statusCode: HttpStatus.OK,
      message: 'Tasks fetched successfully',
      tasks
    };
  }

  @Post()
  async createTasks(@Body() data: TasksDTO) {
    console.log(data, 'data');
    const task = await this.tasksService.create(data);
    return {
      statusCode: HttpStatus.CREATED,
      message: 'Task created successfully',
      task
    };
  }

  @Get(':id')
  async readTask(@Param('id') id: number) {
    const data =  await this.tasksService.read(id);
    return {
      statusCode: HttpStatus.OK,
      message: 'Task fetched successfully',
      data,
    };
  }

  @Patch(':id')
  async updateTask(@Param('id') id: number, @Body() data: Partial<TasksDTO>) {
    await this.tasksService.update(id, data);
    return {
      statusCode: HttpStatus.OK,
      message: 'Task updated successfully',
    };
  }

  @Delete(':id')
  async deleteTask(@Param('id') id: number) {
    await this.tasksService.destroy(id);
    return {
      statusCode: HttpStatus.OK,
      message: 'Task deleted successfully',
    };
  }
}