import { NextApiResponse, NextApiRequest } from "next";
import { Project } from "../../interfaces/Project";
import { connectToDatabase } from "../../utils/DatabaseConnector";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const { method, body } = req;
  console.log("HALLO");
  const db = await connectToDatabase();
  const collection = await db.collection("projects");

  switch (method) {
    case "GET":
      const projects = await collection.find().toArray();
      return res.status(200).json(projects);
    case "POST":
      collection.insertOne(body);
      return res.status(200).json(body);

    default:
      res.setHeader("Allow", ["GET"]);
      res.status(405).end(`Method ${method} Not Allowed`);
  }
}