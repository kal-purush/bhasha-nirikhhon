export type Transaction = {
  sender: string,
  receiver: string,
  amount: number,
  date?: Date,
  confirmed?: boolean
}