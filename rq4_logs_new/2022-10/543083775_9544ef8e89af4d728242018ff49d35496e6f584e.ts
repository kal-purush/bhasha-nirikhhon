import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { Task } from './tasks.entity';
import { TasksServices } from './tasks.service';
import { Repository } from 'typeorm';
import { CreateTaskDto } from './dto/create-task';
import { UpdateTaskDto } from './dto/update-task.dto';

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

const deleteResult = { raw: [], affected: 1 };
const createTaskDto: CreateTaskDto = {
  name: 'work out',
};

const oneTask = {
  id: 1,
  name: 'work out',
  isCompleted: false,
};
const updateTaskDto: UpdateTaskDto = {
   name: 'work out',
   isCompleted: true,
 };

 const updatedTask = {
   id: 1,
   name: 'work out',
   isCompleted: true,
  };

describe('TasksService', () => {
  let service: TasksServices;
  let repository: Repository<Task>;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        TasksServices,
        {
          provide: getRepositoryToken(Task),
          useValue: {
            save: jest.fn().mockResolvedValue(oneTask),
            find: jest.fn().mockResolvedValue(taskArray),
            findOne: jest.fn().mockResolvedValue(oneTask),
            delete: jest.fn().mockResolvedValue(deleteResult),
          },
        },
      ],
    }).compile();

    service = module.get<TasksServices>(TasksServices);
    repository = module.get<Repository<Task>>(getRepositoryToken(Task));
  });
  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('create()', () => {
    it('should insert a task', () => {
      const repoSpy = jest.spyOn(repository, 'save');
      expect(service.create(createTaskDto)).resolves.toEqual(oneTask);
      expect(repoSpy).toBeCalledWith(createTaskDto);
    });
  });
  describe('findAll()', () => {
   it('should get an array of tasks', async () => {
     const repoSpy = jest.spyOn(repository, 'find');
     expect(service.findAll()).resolves.toEqual(taskArray);
     expect(repoSpy).toBeCalled();
   });
 });
  describe('findOne()', () => {
   it('should get a task', () => {
     const repoSpy = jest.spyOn(repository, 'findOne');
     expect(service.findOne('1')).resolves.toEqual(oneTask);
     expect(repoSpy).toBeCalledWith({ where: { id: 1 } });
   });
 });
 describe('update()', () => {
   it('should update a task', () => {
     expect(service.update('1', updateTaskDto)).resolves.toEqual(updatedTask);
   });
 });

 describe('delete()', () => {
   it('should delete a task', async () => {
     const repoSpy = jest.spyOn(repository, 'delete')
     expect(service.delete('1')).resolves.toEqual(deleteResult)
     expect(repoSpy).toBeCalledWith('1')
   })
 })
});