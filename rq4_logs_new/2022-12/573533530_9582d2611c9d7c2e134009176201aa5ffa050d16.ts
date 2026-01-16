export interface Original {
  url: string
  mimeType?: string
}

export interface Picture {
  original: Original
  __typename: string
}

export interface Original2 {
  url: string
  mimeType?: string
}

export interface CoverPicture {
  original: Original2
  __typename: string
}

export interface Stats {
  totalFollowers: number
  totalFollowing: number
}

export interface Profile {
  id: string
  name: string
  bio: string
  picture: Picture
  handle: string
  coverPicture: CoverPicture
  ownedBy: string
  stats: Stats
}