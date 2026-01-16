import { Test, TestingModule } from '@nestjs/testing';
import { TasksService } from './tasks.service';
import { TasksController } from './tasks.controller';
import { CreateTaskDto } from './dto/create-task.dto';
import {UpdateTaskDto} from './dto/update-task.dto';

const taskArray = [
   {
     id: 1,
     name: 'work out',
     isCompleted: false,
   },
   {
     id: 2,
     name: 'read books',
     isCompleted: false,
   },
 ];

 const oneTask = {
   id: 1,
   name: 'work out',
   isCompleted: false,
 };

 const updatedTask = {
   id: 1,
   name: 'work out',
   isCompleted: true,
 };

 const deleteResult = { raw: [], affected: 1 };

  const createTaskDto: CreateTaskDto = {
   name: 'work out',
 };

 const updateTaskDto: UpdateTaskDto = {
   name: 'work out',
   isCompleted: true,
 };

describe('TasksService', () => {
  let controller: TasksController;
  let service: TasksService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [TasksController],
      providers: [TasksService,
         {
         provide: TasksService,
         useValue: {
           create: jest.fn().mockResolvedValue(oneTask),
           findAll: jest.fn().mockResolvedValue(taskArray),
           findOne: jest.fn().mockResolvedValue(oneTask),
           update: jest.fn().mockResolvedValue(updatedTask),
           delete: jest.fn().mockResolvedValue(deleteResult),
         },
       },
      ],
    }).compile();

    controller = module.get<TasksController>(TasksController);
    service = module.get<TasksService>(TasksService);
  });
  it('should be defined', () => {
   expect(controller).toBeDefined();
 });

 describe('create()', () => {
   it('should create a task', () => {
     const svcSpy = jest.spyOn(service, 'create');
     expect(controller.create(createTaskDto)).resolves.toEqual(oneTask);
     expect(svcSpy).toBeCalledWith(createTaskDto);
   });
 });
 describe('findAll()', () => {
   it('should get an array of tasks', async () => {
     const svcSpy = jest.spyOn(service, 'findAll');
     expect(controller.findAll()).resolves.toEqual(taskArray);
     expect(svcSpy).toBeCalled();
   });
 });

 describe('findOne()', () => {
   it('should get a task', () => {
     const svcSpy = jest.spyOn(service, 'findOne');
     expect(controller.findOne('1')).resolves.toEqual(oneTask);
     expect(svcSpy).toBeCalledWith('1');
   });
 });
 describe('update()', () => {
   it('should update a task', () => {
     const repoSpy = jest.spyOn(service, 'update');
     expect(controller.update('1', updateTaskDto)).resolves.toEqual(
       updatedTask,
     );
     expect(repoSpy).toBeCalledWith('1', updateTaskDto);
   });
 });

 describe('delete()', () => {
   it('should delete a task', async () => {
     const repoSpy = jest.spyOn(service, 'delete');
     expect(controller.delete('1')).resolves.toEqual(deleteResult);
     expect(repoSpy).toBeCalledWith('1');
   });
 });
});