import { Client } from "pg";
import { Job, JobTimeEntry } from "./types";

export const backUpUser = async (
  user_id_str: string,
  username: string,
  client: Client
) => {
  const user_id: number = parseInt(user_id_str);
  if (!user_id) throw new Error("Not an int user_id_str");
  try {
    const res = await client.query("Select * from users where user_id=$1", [
      user_id,
    ]);
    if (res.rowCount == 0) {
      await client.query(
        "INSERT INTO users(user_id, username) values($1, $2)",
        [user_id, username]
      );
    }
  } catch (error) {
    console.log(error);
    throw new Error("Can not backUp user");
  }
};

export const getJobsFromDb = async (user_id_str: string, client: Client) => {
  const user_id: number = parseInt(user_id_str);
  if (!user_id) throw new Error("Not an int user_id_str");
  try {
    const res = await client.query("Select * from jobs where user_id=$1", [
      user_id,
    ]);
    const rows = res.rows;
    const formatted_rows: Job[] = rows.map((row) => {
      return {
        jobID: row.job_id + "",
        ref: row.ref,
        title: row.title,
        superviser: row.superviser,
        hours: {
          thisMonth: row.thisMonth,
          lastMonth: row.lastMonth,
          total: row.total,
        },
      };
    });
    return formatted_rows;
  } catch (error) {
    console.log("could not get jobs from user_id");
    throw error;
  }
};

// const createJobTable = `
//     CREATE TABLE IF NOT EXISTS "jobs" (
// 	    "job_id" INT NOT NULL,
//         "user_id" INT NOT NULL,
//         "ref" VARCHAR(100) NOT NULL,
//         "title" VARCHAR(100) NOT NULL,
//         "superviser" VARCHAR(100) NOT NULL,
//         "thisMonth" INT NOT NULL,
//         "lastMonth" INT NOT NULL,
//         "total" INT NOT NULL,
// 	    PRIMARY KEY ("job_id")
//     );`;

export const backUpJobs = async (
  res: Job[],
  user_id_str: string,
  client: Client
) => {
  try {
    const user_id = parseInt(user_id_str);
    await res.forEach(async (job) => {
      const job_id = parseInt(job.jobID);
      const res = await client.query(
        "SELECT * from jobs WHERE job_id=$1 AND user_id=$2",
        [job_id, user_id]
      );
      if (res.rowCount != 0) {
        await client.query(
          `UPDATE jobs SET "thisMonth"=$1, "lastMonth"=$2, total=$3 WHERE job_id=$4`,
          [job.hours.thisMonth, job.hours.lastMonth, job.hours.total, job_id]
        );
      } else {
        await client.query(
          `INSERT INTO jobs(job_id, user_id, ref, title, superviser,  "thisMonth", "lastMonth", total) values($1, $2, $3, $4, $5, $6, $7, $8)`,
          [
            job_id,
            user_id,
            job.ref,
            job.title,
            job.superviser,
            job.hours.thisMonth,
            job.hours.lastMonth,
            job.hours.total,
          ]
        );
      }
    });
  } catch (error) {
    console.log(error);
    throw new Error("Can not backUp jobs");
  }
};

export const backUpEntries = async (
  res: JobTimeEntry[],
  client: Client
) => {
  try {
    // TODO: delete those who are not in res
    await res.forEach(async (entry) => {
      const res = await client.query(
        "SELECT * from entries WHERE entry_id=$1",
        [entry.tid]
      );
      if (res.rowCount == 0) {
        await client.query(
          `INSERT INTO entries(entry_id, job_id, user_id, created_by, hours, worked,  "created", "notes") values($1, $2, $3, $4, $5, $6, $7, $8)`,
          [
            parseInt(entry.tid),
            parseInt(entry.jid),
            parseInt(entry.uid),
            0,
            parseInt(entry.hours),
            entry.worked,
            entry.entered,
            entry.notes
          ]
        );
      }
    });
  } catch (error) {
    console.log(error);
    throw new Error("Can not backUp jobs");
  }
};

// CREATE TABLE IF NOT EXISTS "entries" (
//   "entry_id" INT NOT NULL,
//   "job_id" INT NOT NULL,
//     "user_id" INT NOT NULL,
//   "created_by" INT NOT NULL,
//     "hours" INT NOT NULL,
//     "worked" VARCHAR(100) NOT NULL,
//     "created" VARCHAR(100) NOT NULL,
//     "notes" VARCHAR(1000) NOT NULL,

export const getEntriesFromDb = async (user_id_str: string, job_id_str: string, client: Client) => {
  const user_id: number = parseInt(user_id_str);
  const job_id: number = parseInt(job_id_str);
  if (!user_id || !job_id) throw new Error("Not an int user_id_str or job_id_str");
  try {
    const res = await client.query("Select * from entries where user_id=$1 AND job_id=$2", [
      user_id, job_id
    ]);
    const rows = res.rows;
    const formatted_rows: JobTimeEntry[] = rows.map((row) => {
      return {
        tid: row.entry_id + "",
        jid: row.job_id + "",
        uid: row.user_id + "",
        hours: row.hours + "",
        worked: row.worked,
        entered: row.created,
        notes: row.notes
      };
    });
    return formatted_rows;
  } catch (error) {
    console.log("could not get entries from user_id and job_id");
    throw error;
  }
};