import { TransactionStatus } from '@/types/payment'

export const getTransactionStatus = (transactionStatus: TransactionStatus) => {
  switch (transactionStatus) {
    case 'authorize':
    case 'capture':
    case 'settlement':
      return 'success'
    case 'pending':
    case 'chargeback':
    case 'partial_chargeback':
      return 'pending'
    case 'deny':
    case 'cancel':
    case 'refund':
    case 'partial_refund':
    case 'expire':
    case 'failure':
      return 'error'
    default:
      return 'pending'
  }
}