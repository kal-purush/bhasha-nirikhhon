export interface Post {
  id?: string | number
  userId?: string | number
  username: string
  avatar?: string
  shopName: string
  shopId: string | number
  images: string[]
  comments: number
  date: string
  text?: string
  likes: number
  didLike?: boolean
  premium: boolean
}