import { fetchApi } from "@/utils/http"
import { BankAccount, Company, CostCenter, LocationData, ResPartner } from "@/utils/type"

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

export async function getAllBankAccounts() {
  return fetchApi<BankAccount[]>({
    url: 'api/bank-accounts/get-all-bank-accounts',
    method: 'GET',
  })
}

//need to change the type. it should be chartOfAccount type. not BankAccount type.
export async function getAllChartOfAccounts() { 
  return fetchApi<BankAccount[]>({
    url: 'api/chart-of-accounts/get-all-coa',
    method: 'GET',
  })
}

export async function getAllCostCenters() {
  return fetchApi<CostCenter>({
    url: 'api/cost-centers/get-all-cost-centers',
    method: 'GET',
  })
}

export async function getAllResPartners() {
  return fetchApi<ResPartner>({
    url: 'api/res-partner/get-all-res-partners',
    method: 'GET',
  })
}