// GET /api/analytics?address=0x...
import { NextApiRequest, NextApiResponse } from "next";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const { address } = req.query;
  // TODO: real DB queries
  const totalUrls = await prisma.link.count({ where: { owner: String(address) }});
  const totalClicks = await prisma.link.aggregate({
    where: { owner: String(address) },
    _sum: { clicks: true },
  });
  res.status(200).json({ totalUrls, totalClicks: totalClicks._sum.clicks || 0 });
}