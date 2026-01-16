import { BadRequestException, Controller, NotFoundException, Param, ParseIntPipe, Put, UseGuards } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';

import { PROJECT_USER_ROLE, PTUProject, PTURoles, PTURolesGuard } from '../../guards/ptu-roles.guard';
import { Project } from '../project/project.entity';
import { TASK_STATE } from '../task/task-state.enum';
import { DTask } from '../task/task.dto';
import { TaskService } from '../task/task.service';
import { AuthUser } from '../user/auth/jwt.strategy';
import { User } from '../user/user.entity';
import { TaskTimeService } from './task-time.service';

@Controller()
@UseGuards(AuthGuard('jwt'), PTURolesGuard)
export class TaskTimeController {
  constructor(private taskTimeService: TaskTimeService, private taskService: TaskService) {}

  @Put(':taskId/start')
  @PTURoles(PROJECT_USER_ROLE.DEVELOPER)
  async startTaskTime(
    @Param('taskId', new ParseIntPipe()) taskId: number,
    @PTUProject() project: Project,
    @AuthUser() user: User,
  ) {
    const task = await this.taskService.findById(taskId, project.id);
    if (!task) {
      throw new NotFoundException(`Task with id ${taskId} not found.`);
    } else if (task.userId !== user.id) {
      throw new BadRequestException('You are not assigned to this task.');
    } else if (task.state !== TASK_STATE.ASSIGNED) {
      throw new BadRequestException('You can only make ASSIGNED task ACTIVE.');
    }

    await this.taskTimeService.startTaskTime(task, user);

    task.state = TASK_STATE.ACTIVE;
    await this.taskService.updateTask(task);

    return new DTask(task);
  }

  @Put(':taskId/stop')
  @PTURoles(PROJECT_USER_ROLE.DEVELOPER)
  async endTaskTime(
    @Param('taskId', new ParseIntPipe()) taskId: number,
    @PTUProject() project: Project,
    @AuthUser() user: User,
  ) {
    const task = await this.taskService.findById(taskId, project.id);
    if (!task) {
      throw new NotFoundException(`Task with id ${taskId} not found.`);
    } else if (task.userId !== user.id) {
      throw new BadRequestException('You are not assigned to this task.');
    } else if (task.state !== TASK_STATE.ACTIVE) {
      throw new BadRequestException('You can only end ACTIVE task.');
    }

    await this.taskTimeService.endTaskTime(task, user);

    task.state = TASK_STATE.ASSIGNED;
    await this.taskService.updateTask(task);

    return new DTask(task);
  }
}