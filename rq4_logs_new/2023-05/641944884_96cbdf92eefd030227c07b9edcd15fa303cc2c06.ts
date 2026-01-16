import { Injectable } from '@nestjs/common';
import { Employee } from './entities/employee.entity';
import { CreateEmployeeDto } from './dto/create-employee.dto';
import { UpdateEmployeeDto } from './dto/update-employee.dto';
import { Repository } from 'typeorm';
import { InjectRepository } from '@nestjs/typeorm';
import { CUService } from 'src/utils/CUService';

@Injectable()
export class EmployeeService extends CUService<
  Employee,
  'user' | 'organization',
  CreateEmployeeDto,
  UpdateEmployeeDto
> {
  constructor(
    @InjectRepository(Employee)
    repository: Repository<Employee>,
  ) {
    super(Employee, repository, { entityName: 'Employee', identityKeys: ['user', 'organization'] });
  }
}