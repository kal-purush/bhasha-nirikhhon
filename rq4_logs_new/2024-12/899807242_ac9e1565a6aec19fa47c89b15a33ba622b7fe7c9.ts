// app/api/dashboard-routes/company/overview.ts
import { NextApiRequest, NextApiResponse } from 'next'
import prisma from "@/lib/db";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  try {
    const companies = await prisma.person.findMany({
      where: {
        role: 'Client',
      },
      select: {
        name: true,
        _count: {
          select: {
            created: {
              where: {
                status: 'Closed',
              },
            },
            assigned: {
              where: {
                status: {
                  not: 'Closed',
                },
              },
            },
          },
        },
      },
    })

    const companyData = companies.map(company => ({
      name: company.name,
      completed: company._count.created,
      pending: company._count.assigned,
    }))

    res.status(200).json(companyData)
  } catch (error) {
    console.error(error)
    res.status(500).json({ error: 'Error fetching company overview' })
  }
}