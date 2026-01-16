import { NextApiRequest, NextApiResponse } from 'next'
import axios from 'axios'

const handler = async (
  req: NextApiRequest,
  res: NextApiResponse
): Promise<void> => {
  if (req.method !== 'GET') {
    res.status(405).end()
    return
  }

  try {
    const { handle } = req.query

    const response = await axios
      .get('/membership/revenue' + '?' + 'handle=' + handle)
      .then((res) => res.data)

    return res.status(200).json(response)
  } catch (error) {
    console.log(error)
    return res.status(500).json(error ?? { error: 'Unxpected error occured' })
  }
}

export default handler