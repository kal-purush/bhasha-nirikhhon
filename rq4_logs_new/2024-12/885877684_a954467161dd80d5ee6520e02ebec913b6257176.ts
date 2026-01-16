import { fetchApi } from '@/utils/http'
import { Company } from '@/utils/type'
import { z } from 'zod'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL

export const numberSeriesSchema = z
  .object({
    id: z.number().optional(),
    companyId: z.number().int().positive('Company ID is required'),
    locationId: z.number().int().positive('Location ID is required'),
    voucherType: z.string().min(1, 'Voucher type is required').max(50),
    financialYear: z.number().int().positive('Financial year is required'),
    seriesFormat: z.string().min(1, 'Series format is required').max(255),
    startingNumber: z
      .number()
      .int()
      .positive('Starting number must be positive'),
    endingNumber: z.number().int().positive('Ending number must be positive'),
    createdBy: z.number().optional(),
    currentNumber: z.number().optional(),
  })
  .refine((data) => data.endingNumber >= data.startingNumber, {
    message: 'Ending number must be greater than or equal to starting number',
    path: ['endingNumber'],
  })

const locationSchema = z.object({
  locationId: z.number(),
  address: z.string().min(1, 'Location address is required'),
})

export type NumberSeries = z.infer<typeof numberSeriesSchema>
export type LocationData = z.infer<typeof locationSchema>

export const numberSeriesArraySchema = z.array(numberSeriesSchema)

export async function getAllNumberSeries() {
  return fetchApi<NumberSeries[]>({
    url: 'api/number-series/getAll',
    method: 'GET',
  })
}

export async function createNumberSeries(data: Omit<NumberSeries, 'id'>) {
  console.log('Creating number series:', data)
  return fetchApi<NumberSeries>({
    url: 'api/number-series/entry',
    method: 'POST',
    body: data,
  })
}

export async function updateNumberSeries(data: NumberSeries) {
  console.log('Updating number series:', data)
  return fetchApi<NumberSeries>({
    url: `api/number-series/update/${data.id}`,
    method: 'PATCH',
    body: data,
  })
}

export async function deleteNumberSeries(id: number) {
  console.log('Deleting number series:', id)
  return fetchApi<{ success: boolean }>({
    url: `api/number-series/delete/${id}`,
    method: 'DELETE',
  })
}

export async function getAllCompanies() {
  return fetchApi<Company[]>({
    url: 'api/company/get-all-companies',
    method: 'GET',
  })
}

export async function getAllLocations() {
  return fetchApi<LocationData[]>({
    url: 'api/location/get-all-locations',
    method: 'GET',
  })
}