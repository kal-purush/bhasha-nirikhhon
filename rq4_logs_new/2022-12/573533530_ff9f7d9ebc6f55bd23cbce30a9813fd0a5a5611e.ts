import { NextApiRequest, NextApiResponse } from 'next'
import axios from 'axios'

const handler = async (req: NextApiRequest, res: NextApiResponse) => {
  if (req.method !== 'GET') {
    res.status(405).end()
    return
  }

  try {
    const query = `{
      following(request: {address: "0x3A5bd1E37b099aE3386D13947b6a90d97675e5e3", limit: 10 }) {
        items {
            profile {
              id
              name
              bio
              metadata
              handle
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
                    width
                    height
                    mimeType
                  }
                  medium {
                    url
                    width
                    height
                    mimeType
                  }
                  small {
                    url
                    width
                    height
                    mimeType
                  }
                }
              }
              ownedBy
              stats {
                totalFollowers
                totalFollowing
              }
            }
          }
          pageInfo {
            prev
            next
            totalCount
          }
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