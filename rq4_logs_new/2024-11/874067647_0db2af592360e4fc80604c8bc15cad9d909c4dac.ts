// pages/api/trash.js

import { NextRequest, NextResponse } from "next/server";
import { Pool } from 'pg';

export const revalidate = 0

function sanitizeSQL(query: string) {
  // List of common DDL keywords
  const ddlKeywords = /\b(ALTER|CREATE|DROP|TRUNCATE|RENAME|COMMENT|GRANT|REVOKE|LOCK|REINDEX|CLUSTER|VACUUM|BEGIN|END|DECLARE|EXPLAIN)\b/i;

  // Check if the query contains any DDL commands
  if (ddlKeywords.test(query)) {
      throw new Error('DDL commands are not allowed');
  }

  // If no DDL commands are detected, return the query (safe to execute)
  return query;
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

export async function GET(req: NextRequest) {

  if (req.method === 'GET') {
    const id: string | null = req.nextUrl.searchParams.get("id");

    const query = sanitizeSQL(`SELECT * FROM trash WHERE id = '${id}'`);
    console.log(query);

    try {
      const result = await pool.query(query);
      if (result.rows.length > 0) {
        return NextResponse.json(
          { content: result.rows },
          {
            status: 200,
          }
        );
      } else {
        console.log(result);
        return NextResponse.json(
          { error: 'Content not found' },
          {
            status: 404,
          }
        );
      }
    } catch (error) {
      console.error(error);
      return NextResponse.json(
        { error: 'An error occurred' },
        {
          status: 500,
        }
      );
    }
  } else {
    return NextResponse.json(
      { error: 'Method not allowed' },
      {
        status: 405,
      }
    );
  }
}