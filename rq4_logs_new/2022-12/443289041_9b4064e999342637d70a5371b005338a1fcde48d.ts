import type { EmployeeRepository } from '@/domain/repository/EmployeeRepository'

export type ActionConfigure = {
  employeeRepository: EmployeeRepository
}