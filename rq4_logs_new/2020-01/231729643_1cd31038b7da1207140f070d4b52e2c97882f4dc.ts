import axios from 'axios'

export async function getSearch({ query }: SearchParams) {
  // Generic 을 통해 응답 데이터의 타입을 설정 할 수 있습니다.
  const response = await axios.get<SearchResult>(`https://music.heewon.dev/search`, {
    params: { query },
  })
  return response.data // 데이터 값을 바로 반환하도록 처리합니다.
}

export interface SearchResult {
  result: { music: Array<any>; album: Array<Album>; artist: Array<Artist> }
}
export interface SearchParams {
  query: string
}
export interface Music {
  updatedAt?: string
  createdAt?: string
  id: number
  name: string
  order?: number
  type?: string
  isTitle?: true
  album: Album
  file?: {
    updatedAt: string
    createdAt: string
    id: number
    url: string
    read: number
  }
  artist: Artist
}
export interface Artist {
  updatedAt: string
  createdAt: string
  id: number
  name: string
  photo: string
  userId: number
}
export interface Album {
  updatedAt: string
  createdAt: string
  id: number
  name: string
  photo: string
  artist: Artist
}