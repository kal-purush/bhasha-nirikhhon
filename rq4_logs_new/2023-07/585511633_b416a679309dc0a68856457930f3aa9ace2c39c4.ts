export const selectTransactions = ({ transactions }) =>
  transactions?.transactions || []

export const selectTransactionsByHash =
  hash =>
  ({ transactions }) =>
    transactions?.transactions.find(t => t.hash === hash) || {}