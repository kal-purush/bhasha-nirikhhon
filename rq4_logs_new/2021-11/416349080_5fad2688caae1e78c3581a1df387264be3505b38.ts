import type { NextApiRequest, NextApiResponse } from "next";
import { Api, ResultError } from "../../../api";

const handler = async (req: NextApiRequest, res: NextApiResponse) => {
  const api = new Api();

  const result = await api.createUser({
    ...req.body,
  });

  console.log(`@@@@@@ result`, result);

  if ((<ResultError>result).message !== undefined) {
    const { status, message } = result as ResultError;
    res.status(status).json({ message: message });
  } else {
    res.status(201).json(result);
  }
};

export default handler;