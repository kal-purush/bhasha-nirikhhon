import 'server-only';
import { TRACKERS } from '../../../../database/tables';
import sqlite3 from 'sqlite3';
import type { Nullable } from '@/lib/utils/typeHelpers';
import type { TrackerDefinition } from '../../../lib/utils/types/Tracker';
import { fetchFirst } from '../../../../database/helpers';
import type { NextRequest } from 'next/server';
import { NextResponse } from 'next/server';
import { APIResponse } from '../../../lib/sdk/api';

export async function GET(req: NextRequest) {
  const params = req.nextUrl.searchParams;
  const id = params.get('id');
  let tracker: Nullable<TrackerDefinition> = null;
  const db = new sqlite3.Database('collection.db', sqlite3.OPEN_READONLY);
  const sql = `SELECT * FROM ${TRACKERS} WHERE id = ${id}`;

  try {
    tracker = await fetchFirst(db, sql);
  } catch (err) {
    console.log(err);
  } finally {
    db.close();
  }

  if (tracker)
    return NextResponse.json(
      new APIResponse<TrackerDefinition>({ success: true, data: tracker }),
    );
  // TODO: error processor for error, empty, etc
  return NextResponse.json(
    new APIResponse<TrackerDefinition>({ success: false }),
  );
}