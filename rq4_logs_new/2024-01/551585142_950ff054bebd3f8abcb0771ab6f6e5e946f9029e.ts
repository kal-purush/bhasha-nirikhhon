import prisma from "@database/prisma/instance";
import type { DebugSubmissions } from "@prisma/client";
import { cache } from "react";

import type { Results, UserData } from "@/shared/types";

import type { DetailedScoreInfo } from "./getProblemScore";

export type ProblemScoreMap = Record<number, DetailedScoreInfo>;

export type GetProblemsScoreResult =
  | { ok: true; data: ProblemScoreMap }
  | { ok: false; status: number; error: string };

type SqlRawSubInfo = Pick<DebugSubmissions, "dpid" | "diff" | "score" | "result" | "drid">;

const getProblemsScore = async (user: UserData): Promise<GetProblemsScoreResult> => {
  const tid = user.id;

  const subsInfo = await prisma.$queryRaw<SqlRawSubInfo[]>`
    WITH score_table AS (
      SELECT MAX(ds.score) AS score, ds.dpid
      FROM debug_submissions ds
      WHERE ds.tid = ${tid}
      GROUP BY ds.dpid
    )
    SELECT ds.dpid, ds.diff, st.score, ds.result, ds.drid
    FROM score_table st
    JOIN debug_submissions ds ON ds.score = st.score AND ds.dpid = st.dpid
    WHERE ds.tid = ${tid}
    GROUP BY ds.dpid
  `;

  const result: ProblemScoreMap = {};

  for (const { dpid, result: subResult, score, ...subInfo } of subsInfo) {
    result[dpid] = {
      score,
      result: subResult as Results,
      ...subInfo,
    };
  }

  return { ok: true, data: result };
};

export const getMemoProblemsScore = cache(getProblemsScore);