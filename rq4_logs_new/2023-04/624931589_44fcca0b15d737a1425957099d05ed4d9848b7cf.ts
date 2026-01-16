import { NextApiResponse, NextApiRequest } from "next";
import { connectToDatabase } from "../../../utils/DatabaseConnector";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const { method, body } = req;
  const db = await connectToDatabase();
  const collection = await db.collection("projects");

  switch (method) {
    case "PUT":
      const projects = await collection.find().toArray();
      return res.status(200).json(projects);

    default:
      res.setHeader("Allow", ["GET", "POST"]);
      res.status(405).end(`Method ${method} Not Allowed`);
  }
}