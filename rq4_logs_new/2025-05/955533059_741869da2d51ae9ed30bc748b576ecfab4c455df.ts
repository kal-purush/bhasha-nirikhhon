import type sqlite3 from 'sqlite3';

export async function fetchAll<T>(
  db: sqlite3.Database,
  sql: string,
  params?: Array<unknown>,
): Promise<Array<T>> {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      resolve(rows as T[]);
    });
  });
}

export async function fetchFirst<T>(
  db: sqlite3.Database,
  sql: string,
  params?: Array<unknown>,
): Promise<Array<T>> {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      resolve(row as T[]);
    });
  });
}