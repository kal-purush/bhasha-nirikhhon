import { useContext } from "react"
import { TransactionsContext } from "../contexts/transactions-context"

export function useSummary() {
  const { transactions } = useContext(TransactionsContext)
  const summary = transactions.reduce((acc, transaction) => {

    if (transaction.type === 'income') {
      acc.income += transaction.price
      acc.total += transaction.price
    } else {
      acc.outcome -= transaction.price
      acc.total -= transaction.price
    }
    return acc
  }, {
    outcome: 0,
    income: 0,
    total: 0
  })

  return summary
}