// Next.js API route support: https://nextjs.org/docs/api-routes/introduction
import type { NextApiRequest, NextApiResponse } from 'next'
import { client } from '@/lib/graphql'
import { Memory } from '@/graphql/types/GetMemory'
import { createMemory } from '@/graphql/mutations/addMemory'

type Data = {
  success: boolean
}

type Body = {
  eventId: string
  name: string
  story: string
}

/** This function will send to the GraphCMS the mutation */
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<Data>
) {
  const { name, story, eventId } = req.body as Body

  client.setHeaders({
    Authorization: `Bearer ${process.env.GRAPHCMS_MUTATION_TOKEN}`
  })

  await client.request<Memory, Body>(createMemory, {
    name,
    story,
    eventId
  })

  res.status(200).json({ success: true })
}