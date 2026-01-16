import type { NextApiRequest, NextApiResponse } from "next";
import fs from "fs/promises";

type ResponseData = {};

export default async function handler(req: NextApiRequest, res: NextApiResponse<ResponseData>) {
  try {
    console.log(req.body);

    // create a folder in ../sessions if it doesn't exist
    const sessionsDir = "../sessions";

    let directoryExists;
    try {
      await fs.access(sessionsDir);
      directoryExists = true;
    } catch {
      directoryExists = false;
    }
    if (!directoryExists) {
      console.log("CREATING SESSION DIR");
      await fs.mkdir(sessionsDir, { recursive: true });
    }

    await fs.writeFile(`../sessions/${req.body.uuid}.json`, JSON.stringify(req.body, null, 2));

    res.status(200).end();
  } catch (err) {
    console.log(err);
    res.status(500).end();
  }
}