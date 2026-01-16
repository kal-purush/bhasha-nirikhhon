import { fetchApi } from '@/utils/http'
import { z } from 'zod'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL

// Define the department schema based on the previously provided structure
export const departmentSchema = z.object({
  departmentId: z.number().min(1, 'Department id is required'),
  departmentName: z.string().min(1, 'Department name is required'),
  budget: z.string().optional(),
  currencyCode: z.number().optional(),
  isActive: z.boolean().optional(),
  startDate: z.coerce.date().optional(),
  endDate: z.coerce.date().optional(),
  actual: z.string().optional(),
})

export type Department = z.infer<typeof departmentSchema>

export const departmentsArraySchema = z.array(departmentSchema)

export async function getAllDepartments() {
  return fetchApi<Department[]>({
    url: 'api/departments/get-all-departments',
    method: 'GET',
  })
}

export async function createDepartment(data: Omit<Department, 'departmentId'>) {
  console.log('Creating department:', data)
  return fetchApi<Department>({
    url: 'api/departments/create-department',
    method: 'POST',
    body: data,
  })
}
