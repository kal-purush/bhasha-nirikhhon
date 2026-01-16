import type { NextApiRequest, NextApiResponse } from "next";
import query from "../../../engine/query";

type ResponseData = {};

export default async function handler(req: NextApiRequest, res: NextApiResponse<ResponseData>) {
  try {
    // TODO: consider sqli
    const [schema, tableName] = (req.query.tableName as string).split(".");
    const sql = `select * from ${schema}.${tableName} limit 5`;
    const results = await query(
      sql,
      { databaseType: process.env.DATABASE_TYPE },
      [],
    );

    res.status(200).json({ sql, results });
  } catch (err) {
    console.log(err);
    res.status(500).end();
  }
}