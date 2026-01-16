import { Body, Controller, HttpCode, HttpStatus, NotAcceptableException, Post } from '@nestjs/common';
import { MainService } from '../main.service';
import CreateTaskDto from './dto/createtask.dto';

@Controller('task')
export class TaskController {
  constructor(private readonly mainService: MainService) {}

  /**
   * Creates a new task
   * 
   * @param createDto the body of the request
   * @returns the newly created task
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  async create(@Body() createDto: CreateTaskDto) {
    const username = 'wonder1';
    const task = await this.mainService.setUpTask(createDto, username);
    if (!task) throw NotAcceptableException;
    return task;
  }
}