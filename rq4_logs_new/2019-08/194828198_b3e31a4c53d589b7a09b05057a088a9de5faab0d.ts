// ref: https://github.com/zeit/next.js/blob/canary/examples/with-mobx/store.js
import { action, observable } from 'mobx'
import { useStaticRendering } from 'mobx-react'

interface InitialData {
  lastUpdate: number
  light: boolean
}

const isServer = typeof window === 'undefined'
useStaticRendering(isServer)

export class Store {
  @observable public lastUpdate = 0

  @observable public light = false

  public timer = 0

  // @ts-ignore: disable noUnusedParameters because it is example
  public constructor(isServer: boolean, initialData: Partial<InitialData> = {}) {
    this.lastUpdate = initialData.lastUpdate != null ? initialData.lastUpdate : Date.now()
    this.light = !!initialData.light
  }

  @action public start = () => {
    this.timer = setInterval(() => {
      this.lastUpdate = Date.now()
      this.light = true
    }, 1000)
  }

  public stop = () => clearInterval(this.timer)
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let store: Store = null as any

export function initializeStore(initialData: Partial<InitialData> = {}) {
  // Always make a new store if server, otherwise state is shared between requests
  if (isServer) {
    return new Store(isServer, initialData)
  }
  if (store === null) {
    store = new Store(isServer, initialData)
  }
  return store
}