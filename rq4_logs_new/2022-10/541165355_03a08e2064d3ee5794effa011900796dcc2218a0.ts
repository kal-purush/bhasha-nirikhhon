import { ObjectId } from 'mongodb'
import type { NextApiRequest, NextApiResponse } from 'next'
import { unstable_getServerSession } from 'next-auth'
import { getToken } from 'next-auth/jwt'
import { getProviders, getSession } from 'next-auth/react'
import clientPromise from 'src/lib/mongodb'
import { authOptions } from '../auth/[...nextauth]'

export default async function handler(req, res) {
  const { method } = req

  const session = await unstable_getServerSession(req, res, authOptions)

  // Get data from your database
  const client = await clientPromise

  const db = client.db('pepper')
  const recipes = db.collection('recipes')

  if (method == 'POST') {
    const result = await recipes.insertOne({
      user: {
        name: session.user?.name,
        email: session.user?.email,
      },
      status: 0,
    })

    res.status(200).json(result)
  } else {
    res.status(405)
  }
}