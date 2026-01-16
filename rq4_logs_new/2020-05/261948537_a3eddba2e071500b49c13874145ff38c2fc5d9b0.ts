import { Source } from 'models/source'
import { Content, HasContent } from 'models/content'
import { HasUser } from 'models/hasUser'

export interface Post extends HasUser, HasContent {
  created_at: Date
  id: string
  is_deleted?: boolean
  is_nsfw: boolean
  is_revised?: boolean
  revision?: string
  source: Source
  thread_id: string
  reply_to: string
  repost_of?: Post
  counts: {
    bookmarks: number
    replies: number
    reposts: number
    threads: number
  }
  content?: Post.PostContent
  raw?: any[]
}

export namespace Post {
  export interface PostContent extends Content {
    links_not_parsed: boolean
  }
}