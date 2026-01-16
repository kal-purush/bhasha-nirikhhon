import fetch from 'node-fetch'
import { CreateAppStreamBody, CreateAppStreamResponse } from './types'

export async function createAppStream(): Promise<CreateAppStreamResponse> {
  const body: CreateAppStreamBody = {
    type: 'long_poll',
    object_types: [
      'post',
      'bookmark',
      'follow',
      'mute',
      'block',
      'message',
      'channel',
      'channel_subscription',
      'token',
      'file',
      'poll',
      'user',
    ],
  }
  return fetch('https://api.pnut.io/v0/streams', {
    body: JSON.stringify(body),
  }).then((res) => res.json())
}