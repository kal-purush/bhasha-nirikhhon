import { z } from 'zod'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL

// Define the schema for a single res partner with more lenient validation
export const resPartnerSchema = z.object({
  id: z.number().int().positive(),
  name: z.string().max(255).nullable().optional(),
  displayName: z.string().max(255).nullable().optional(),
  companyName: z.string().max(255).nullable().optional(),
  type: z.string().max(255).nullable().optional(),
  companyId: z.number().int().positive().nullable().optional(),
  email: z.string().email().max(100).nullable().optional(),
  phone: z.string().max(20).nullable().optional(),
  mobile: z.string().max(20).nullable().optional(),
  website: z.string().url().max(255).nullable().optional().or(z.literal("")),
  isCompany: z.boolean().nullable().optional().default(true),
  vat: z.string().max(100).nullable().optional(),
  street: z.string().max(255).nullable().optional(),
  city: z.string().max(100).nullable().optional(),
  zip: z.string().max(20).nullable().optional(),
  active: z.boolean().nullable().optional().default(true),
  creditLimit: z.number().multipleOf(0.01).nonnegative().nullable().optional(),
  customerRank: z.number().int().nonnegative().nullable().optional(),
  supplierRank: z.number().int().nonnegative().nullable().optional(),
  comment: z.string().nullable().optional(),
  createdBy: z.number().int().positive().nullable().optional(),
  createdAt: z.string().nullable().optional(), // Changed from date to string
  updatedBy: z.number().int().positive().nullable().optional(),
  updatedAt: z.string().nullable().optional(), // Changed from date to string
})

export type ResPartner = z.infer<typeof resPartnerSchema>

export const resPartnersArraySchema = z.array(resPartnerSchema)

// Schema for creating a new res partner (omitting id, createdAt, and updatedAt)
export const createResPartnerSchema = resPartnerSchema.omit({
  id: true,
  createdAt: true,
  updatedAt: true,
})

// Schema for updating an existing res partner (making all fields optional except id)
export const updateResPartnerSchema = resPartnerSchema.partial().required({ id: true })

export async function getAllResPartners(): Promise<ResPartner[]> {
  console.log('API: Fetching all res partners')

  const response = await fetch(
    `${API_BASE_URL}/api/res-parter/get-all-res-partners`
  )

  console.log('API: Get all res partners response status:', response.status)
  const responseData = await response.json()
  console.log('API: Get all res partners response data:', responseData.data)

  if (!response.ok) {
    throw new Error(responseData.message || 'Failed to fetch res partners')
  }

  try {
    // Transform the data to ensure it matches our schema
    const transformedData = responseData.data.map((item: any) => ({
      ...item,
      // Ensure numbers are properly typed
      id: Number(item.id),
      companyId: item.companyId ? Number(item.companyId) : null,
      creditLimit: item.creditLimit ? Number(item.creditLimit) : null,
      customerRank: item.customerRank ? Number(item.customerRank) : null,
      supplierRank: item.supplierRank ? Number(item.supplierRank) : null,
      // Convert string booleans to actual booleans if needed
      isCompany: item.isCompany === 'true' ? true : item.isCompany === 'false' ? false : item.isCompany,
      active: item.active === 'true' ? true : item.active === 'false' ? false : item.active,
    }))

    // Validate the transformed data
    const validatedData = resPartnersArraySchema.parse(transformedData)
    return validatedData
  } catch (error) {
    console.error('Validation error:', error)
    throw error
  }
}

export async function createResPartner(resPartner: z.infer<typeof createResPartnerSchema>): Promise<ResPartner> {
  console.log('API: Creating res partner with data:', resPartner)

  const response = await fetch(
    `${API_BASE_URL}/api/res-parter/create-res-partner`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(resPartner),
    }
  )

  console.log('API: Res partner creation response status:', response.status)
  const responseData = await response.json()
  console.log('API: Res partner creation response data:', responseData.data)

  if (!response.ok) {
    throw new Error(responseData.message || 'Failed to create res partner')
  }

  // Transform and validate the response data
  const transformedData = {
    ...responseData.data,
    id: Number(responseData.data.id),
    companyId: responseData.data.companyId ? Number(responseData.data.companyId) : null,
    creditLimit: responseData.data.creditLimit ? Number(responseData.data.creditLimit) : null,
    customerRank: responseData.data.customerRank ? Number(responseData.data.customerRank) : null,
    supplierRank: responseData.data.supplierRank ? Number(responseData.data.supplierRank) : null,
  }

  const validatedData = resPartnerSchema.parse(transformedData)
  return validatedData
}

export async function updateResPartner(resPartner: z.infer<typeof updateResPartnerSchema>): Promise<ResPartner> {
  console.log('API: Updating res partner with data:', resPartner)

  const response = await fetch(
    `${API_BASE_URL}/api/res-parter/edit-res-partner/${resPartner.id}`,
    {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(resPartner),
    }
  )

  console.log('API: Res partner update response status:', response.status)
  const responseData = await response.json()
  console.log('API: Res partner update response data:', responseData.data)

  if (!response.ok) {
    throw new Error(responseData.message || 'Failed to update res partner')
  }

  // Transform and validate the response data
  const transformedData = {
    ...responseData.data,
    id: Number(responseData.data.id),
    companyId: responseData.data.companyId ? Number(responseData.data.companyId) : null,
    creditLimit: responseData.data.creditLimit ? Number(responseData.data.creditLimit) : null,
    customerRank: responseData.data.customerRank ? Number(responseData.data.customerRank) : null,
    supplierRank: responseData.data.supplierRank ? Number(responseData.data.supplierRank) : null,
  }

  const validatedData = resPartnerSchema.parse(transformedData)
  return validatedData
}