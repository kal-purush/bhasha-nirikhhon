import { fetchApi } from '@/utils/http'
import {
  BankAccount,
  ChartOfAccount,
  Company,
  CostCenter,
  Department,
  JournalEntryWithDetails,
  JournalQuery,
  LocationData,
  VoucherById,
} from '@/utils/type'

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

export async function getAllChartOfAccounts() {
  return fetchApi<ChartOfAccount[]>({
    url: 'api/chart-of-accounts/get-all-coa',
    method: 'GET',
  })
}

export async function getSingleVoucher(voucherid: string) {
  console.log(voucherid)
  return fetchApi<VoucherById[]>({
    url: `api/journal/getJournalDetail/${voucherid}`,
    method: 'GET',
  })
}

export async function reverseJournalVoucher(
  voucherNo: number,
  createdId: number
) {
  console.log(
    '🚀 ~ reverseJournalVoucher ~ voucherNo: number, createdId: number:',
    voucherNo,
    createdId
  )
  return fetchApi<VoucherById[]>({
    url: `api/journal/reverseEntry`,
    method: 'POST',
    body: { voucherNo, createdId },
    headers: {
      'Content-Type': 'application/json',
    },
  })
}

export async function editJournalVoucher(voucherid: number, createid: number) {
  console.log(voucherid, createid)
  return fetchApi<VoucherById[]>({
    url: `api/journal/postJournal/${voucherid}/${createid}`,
    method: 'POST',
  })
}

export async function getAllVoucher(data: JournalQuery) {
  const queryParams = new URLSearchParams({
    date: data.date,
    companyId: JSON.stringify(data.companyId), // Convert array to JSON string
    locationId: JSON.stringify(data.locationId), // Convert array to JSON string
    voucherType: data.voucherType,
  }).toString()
  console.log(queryParams)
  return fetchApi({
    url: `api/journal/getJournalDetails/?${queryParams}`,
    method: 'GET',
  })
}

export async function createJournalEntryWithDetails(
  data: JournalEntryWithDetails
) {
  console.log('Under APi:')
  console.log(data)
  return fetchApi<JournalEntryWithDetails>({
    url: 'api/journal/entry',
    method: 'POST',
    body: data,
  })
}

export async function getAllBankAccounts() {
  return fetchApi<BankAccount[]>({
    url: 'api/bank-accounts/get-all-bank-accounts',
    method: 'GET',
  })
}