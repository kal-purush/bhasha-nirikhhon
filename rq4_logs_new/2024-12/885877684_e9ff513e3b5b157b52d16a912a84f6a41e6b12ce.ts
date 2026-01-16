import { fetchApi } from '@/utils/http'
import { BankAccountDateRange } from '@/utils/type'

export async function getBankAccountsByDate(params: BankAccountDateRange) {
  return fetchApi<BankAccountDateRange[]>({
    url: `api/ledgerrepot/bank-ledger`,
    method: 'GET',
    body: params,
    headers: {
      'Content-Type': 'application/json',
    },
  })
}