import { $, QRL } from '@builder.io/qwik'

export const getSession: QRL<(nameItem: string) => any> = $((nameItem) => {
  const session = sessionStorage.getItem(nameItem)
  if (session) {
    return JSON.parse(session)
  }
  return null
})

export const setSession: QRL<
  ({ nameItem, value }: { nameItem: string; value: any }) => void
> = $(({ nameItem, value }) => {
  sessionStorage.setItem(nameItem, JSON.stringify(value))
})

export const removeSession: QRL<(nameItem: string) => void> = $((nameItem) => {
  sessionStorage.removeItem(nameItem)
})