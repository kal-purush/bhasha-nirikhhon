import { useQuery } from 'react-query'

export function useAccount() {
  const { data: [account] = [], status } = useQuery('accounts', () =>
    window.ethereum.request<string[]>({ method: 'eth_accounts' })
  )

  return { account, status }
}