'use server';

import type { Nullable } from '../../utils/typeHelpers';
import type { Question } from '../../utils/types/Questions';
import sqlite3 from 'sqlite3';
import { fetchAll } from '../../../../database/helpers';

interface DBQuestion extends Omit<Question, 'data'> {
  data: string;
}

export async function getTrackerQuestions({
  trackerId,
}: {
  trackerId?: Nullable<number>;
}) {
  if (!trackerId) return;
  let questions: Nullable<Question[]> = null;
  const db = new sqlite3.Database('collection.db', sqlite3.OPEN_READONLY);

  const sql = `WITH tq AS (SELECT rank, questionId FROM tracker_questions WHERE trackerId = ${trackerId})
    SELECT q.*, tq.rank FROM questions q
    inner join tq on q.id = tq.questionId
    WHERE q.id IN (SELECT questionId FROM tq)
    ORDER BY rank;`;

  try {
    const res: DBQuestion[] = await fetchAll(db, sql);
    questions = res.map(q => ({
      ...q,
      header: Boolean(q.header),
      preview: Boolean(q.preview),
      data: q.data ? JSON.parse(q.data) : null,
    }));
  } catch (err) {
    console.log(err);
  } finally {
    db.close();
  }
  return questions;
}