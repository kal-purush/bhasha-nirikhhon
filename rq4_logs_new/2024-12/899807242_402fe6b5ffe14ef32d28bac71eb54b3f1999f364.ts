// pages/api/tickets/category-breakdown.ts
import { NextApiRequest, NextApiResponse } from 'next'
import prisma from "@/lib/db";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    const categories = await prisma.ticket.groupBy({
      by: ['type'],
      _count: {
        type: true,
      },
    })

    const categoryData = categories.map(category => ({
      category: category.type,
      count: category._count.type,
    }))

    res.status(200).json(categoryData)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: 'Error fetching category breakdown' })
  }
}