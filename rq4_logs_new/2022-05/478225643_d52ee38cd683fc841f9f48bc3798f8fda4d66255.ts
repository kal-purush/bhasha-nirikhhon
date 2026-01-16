// Next.js API route support: https://nextjs.org/docs/api-routes/introduction
import type { NextApiRequest, NextApiResponse } from 'next'

type Input = {
  question: number
  sessionUuid: string
  value: string | number
}
type Output = {
  errors?: string[]
}

type Session = Record<number, number | string>
const sessions: Record<string, Session> = {}
// TODO: Send sessions object to S3!

export default function handler(
  req: NextApiRequest,
  res: NextApiResponse<Output>
) {
  try {
    const { body: rawBody } = req
    const body: Input = JSON.parse(rawBody)
    const { question, sessionUuid, value } = body

    sessions[sessionUuid] = {
      ...sessions[sessionUuid],
      [question]: value
    }

    res.status(200).json({ errors: [] })
  } catch (e) {
    console.warn(e)
    res.status(400).json({ errors: ['bad request'] })
  }
}