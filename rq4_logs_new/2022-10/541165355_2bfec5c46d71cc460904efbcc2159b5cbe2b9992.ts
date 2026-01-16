import { ObjectId } from 'mongodb'
import type { NextApiRequest, NextApiResponse } from 'next'
import clientPromise from 'src/lib/mongodb'

export default async function handler(_req: NextApiRequest, res: NextApiResponse) {
  const {
    query: { ingredientId },
    method,
  } = _req

  // Get data from your database
  const client = await clientPromise

  const db = client.db('pepper')
  const recipes = db.collection('ingredients')

  if (method == 'DELETE') {
    const result = await recipes.deleteOne({ _id: new ObjectId(ingredientId.toString()) })

    res.status(200).json(result)
  }
}