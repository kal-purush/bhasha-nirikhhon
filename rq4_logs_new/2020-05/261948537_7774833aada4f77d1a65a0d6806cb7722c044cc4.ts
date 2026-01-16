import {
  messageIsPost,
  messageIsMessage,
  messageIsBookmark,
  messageIsFollow,
  StreamMessage,
} from 'src/models/streamMessage'
import { HasUser } from 'src/models/hasUser'
import WebSocket from 'ws'
import debug from 'debug'

export interface BroadcastMessage {
  targetIds: string[]
  message?: {
    meta: {
      type: 'post' | 'message' | 'bookmark' | 'follow'
    }
  }
}

interface BroadcastPostMessage extends BroadcastMessage {
  message: {
    meta: Pick<StreamMessage.PostMessage['meta'], 'type'>
    data: StreamMessage.PostMessage['data']
  }
}

interface BroadcastMessageMessage extends BroadcastMessage {
  message: {
    meta: Pick<StreamMessage.MessageMessage['meta'], 'type'>
    data: StreamMessage.MessageMessage['data']
  }
}

interface BroadcastBookmarkMessage extends BroadcastMessage {
  message: {
    meta: Pick<StreamMessage.BookmarkMessage['meta'], 'type'>
    data: StreamMessage.BookmarkMessage['data']
  }
}

interface BroadcastFollowMessage extends BroadcastMessage {
  message: {
    meta: Pick<StreamMessage.FollowMessage['meta'], 'type'>
    data: StreamMessage.FollowMessage['data']
  }
}

export function toBroadcastMessage(
  data: WebSocket.Data
): BroadcastMessage | undefined {
  const abstractMessage = JSON.parse(data.toString())
  if (messageIsPost(abstractMessage)) {
    if (abstractMessage.data.is_deleted) return
    const res =
      abstractMessage.data.content?.entities.mentions.map(
        (mention) => mention.id
      ) ?? []
    debug('pnut-stream:client:broadcastMessage')(res)
    const broadcastMessage: BroadcastPostMessage = {
      targetIds: res.filter(excludeContributor(abstractMessage.data)),
      message: abstractMessage,
    }
    return broadcastMessage
  }
  if (messageIsMessage(abstractMessage)) {
    const broadcastMessage: BroadcastMessageMessage = {
      targetIds: abstractMessage.meta.subscribed_user_ids.filter(
        excludeContributor(abstractMessage.data)
      ),
      message: abstractMessage,
    }
    return broadcastMessage
  }
  if (messageIsBookmark(abstractMessage)) {
    const broadcastMessage: BroadcastBookmarkMessage = {
      targetIds: [abstractMessage.data.user.id].filter(
        excludeContributor(abstractMessage.data)
      ),
      message: abstractMessage,
    }
    return broadcastMessage
  }
  if (messageIsFollow(abstractMessage)) {
    const broadcastMessage: BroadcastFollowMessage = {
      targetIds: [abstractMessage.data.followed_user.id],
      message: abstractMessage,
    }
    return broadcastMessage
  }
  return { targetIds: [] }
}

function excludeContributor(hasUserObj: HasUser) {
  return (id: string) => {
    return id !== hasUserObj.user?.id
  }
}