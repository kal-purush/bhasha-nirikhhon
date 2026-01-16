import type { NextApiRequest, NextApiResponse } from 'next'
import clientPromise from 'src/lib/mongodb'

export default async function handler(_req: NextApiRequest, res: NextApiResponse) {
  //   console.debug(_req.query)
  const query = _req.query

  // Get data from your database
  const client = await clientPromise

  const db = client.db('pepper')
  const recipes = db.collection('ingredients')

  const result = await recipes.find({ name: { $regex: query.keyword, $options: 'i' } }).toArray()

  res.status(200).json(result)
}