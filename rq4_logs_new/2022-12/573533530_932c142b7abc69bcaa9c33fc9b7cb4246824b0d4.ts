import { NextApiRequest, NextApiResponse } from 'next'
import axios from 'axios'

const handler = async (
  req: NextApiRequest,
  res: NextApiResponse<Profile | { error: string }>
) => {
  if (req.method !== 'GET') {
    res.status(405).end()
    return
  }

  try {
    const { handle } = req.query

    const query = `{
      profile(request: { handle: "${handle}" }) {
        id
        name
        bio
        picture {
          ... on NftImage {
            contractAddress
            tokenId
            uri
            verified
          }
          ... on MediaSet {
            original {
              url
              mimeType
            }
          }
          __typename
        }
        handle
        coverPicture {
          ... on NftImage {
            contractAddress
            tokenId
            uri
            verified
          }
          ... on MediaSet {
            original {
              url
              mimeType
            }
          }
          __typename
        }
        ownedBy
        stats {
          totalFollowers
          totalFollowing
        }
      }
    }`

    const response = await axios
      .post('https://api.lens.dev/', {
        query,
      })
      .then((res) => res.data)

    return res.status(200).json(response)
  } catch (error) {
    console.log(error)
    return res.status(500).json({ error: 'Unxpected error occured' })
  }
}

export default handler

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