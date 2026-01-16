import { fetchApi } from '@/utils/http'
import {
  AccountsHead,
  ChartOfAccount,
  JournalEntryWithDetails,
  JournalEntryWithDetailsSchema,
  JournalQuery,
  JournalResult,
  ResPartner,
} from '@/utils/type'
import { z } from 'zod'
import { costCenterSchema } from './cost-centers-api'

export type CostCenter = z.infer<typeof costCenterSchema>

export async function createJournalEntryWithDetails(
  data: JournalEntryWithDetails
) {
  console.log("Under APi:");
  console.log(data);
  return fetchApi<JournalEntryWithDetails>({
    url: 'api/journal/entry',
    method: 'POST',
    body: data,
  })
}
// Get All Voucher Data
export async function getAllVoucher(data:JournalQuery) {
  const queryParams=new URLSearchParams({
    date: data.date,
    companyId: JSON.stringify(data.companyId), // Convert array to JSON string
    locationId: JSON.stringify(data.locationId), // Convert array to JSON string
    voucherType: data.voucherType,
  }).toString();
  
  return fetchApi<JournalResult[]>({
    url: `api/journal/getJournalDetails/?${queryParams}`,
    method: 'GET',
    
  })
}
// chart of accouts get all data
export async function getAllChartOfAccounts() {
  return fetchApi<AccountsHead[]>({
    url: 'api/chart-of-accounts/get-all-coa',
    method: 'GET',
  })
}

//cost center get all data
export async function getAllCostCenters() {
  return fetchApi<CostCenter[]>({
    url: 'api/cost-centers/get-all-cost-centers',
    method: 'GET',
  })
}

// get all res partner data
export async function getAllResPartners() {
  return fetchApi<ResPartner[]>({
    url: 'api/res-partner/get-all-res-partners',
    method: 'GET',
  })
}