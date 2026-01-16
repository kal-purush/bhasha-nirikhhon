import {createEffect, createSignal} from 'solid-js'
import {Signal} from 'solid-js/types/reactive/signal'
export interface CreateSyncProps<T> {
  value?: T
}
export const createSyncSignal = <T>(props: CreateSyncProps<T>): Signal<T | undefined> => {
  const [signal, setSignal] = createSignal<T | undefined>(props.value)

  createEffect(() => setSignal(() => props.value))

  return [signal, setSignal]
}