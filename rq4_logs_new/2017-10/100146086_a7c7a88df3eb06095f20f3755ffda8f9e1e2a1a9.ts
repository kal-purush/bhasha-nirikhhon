// tslint:disable-next-line:no-reference
/// <reference path="../typings/sse.d.ts" />

import { create } from '@most/create'
import * as most from 'most'

import { getPartyApiHost } from './request'

export interface MessageEvent extends Event {
  data: string
}

export default class Source<T> {
  private eventSource: typeof EventSource

  constructor(private url: string, private messageName: string) {
    this.eventSource = new EventSource(
      `${getPartyApiHost()}${url}`,
      {
        withCredentials: true,
      },
    )
    const opening = this.opened
  }

  static parseMessage<T>(e: MessageEvent) {
    // tslint:disable-next-line:no-console
    console.log('Received message: ', e.data)
    return JSON.parse(e.data) as T
  }

  get opened() {
    return this.on('open').map(e => {
      // tslint:disable-next-line:no-console
      console.log('Opened event stream for ' + this.messageName)
      return e
    })
  }

  get messages() {
    return this.on(this.messageName).map((e: MessageEvent) => Source.parseMessage<T>(e))
  }

  on(eventType: 'open' | 'message' | string) {
    return create((add, end, error) => {
      const addEvent = (e: any) => {
        if (this.eventSource.readyState !== 2 /* sse.ReadyState.CLOSED */) {
          add(e)
        } else {
          end()
        }
      }
      const addError = (e: any) => error(new Error('EventSource failed.'))

      this.eventSource.addEventListener(eventType, addEvent)
      this.eventSource.addEventListener('error', addError)

      return () => {
        this.eventSource.removeEventListener(eventType, addEvent)
        this.eventSource.removeEventListener('error', addError)
      }
    })
  }

  close() {
    this.eventSource.close()
  }
}