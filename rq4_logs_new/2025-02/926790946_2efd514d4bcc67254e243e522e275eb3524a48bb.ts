import { prisma } from "@/lib/prisma";
import { NextApiRequest, NextApiResponse } from "next";

export default async function status(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method Not Allowed." });
  }

  try {
    const serverVersionResponse = await prisma.$queryRaw<{ server_version: number }[]>`SHOW server_version;`;
    const serverVersionData = serverVersionResponse[0].server_version;

    const maxConnectionsResponse = await prisma.$queryRaw<{ max_connections: number }[]>`show max_connections;`;
    const maxConnectionsData = maxConnectionsResponse[0].max_connections;

    const databaseName = process.env.POSTGRES_DB;
    const openConnectionsResponse = await prisma.$queryRaw<
      { count: number }[]
    >`SELECT count(*)::int FROM pg_stat_activity WHERE datname = ${databaseName};`;
    const openConnectionsData = openConnectionsResponse[0].count;

    return res.status(200).json({
      updated_at: new Date().toLocaleString("pt-BR"),
      dependencies: {
        database: {
          server_version: serverVersionData,
          max_connections: Number(maxConnectionsData),
          open_connections: openConnectionsData,
        },
      },
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Internal Server Error." });
  }
}