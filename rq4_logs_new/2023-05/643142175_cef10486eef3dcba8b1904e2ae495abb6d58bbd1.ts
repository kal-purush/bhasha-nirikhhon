import { type NextApiRequest, type NextApiResponse } from "next"
import { env } from "../../env.mjs"
import { MongoClient } from "mongodb"
import type User from "./utils/user"
import { type Cluster, Clusterer } from "k-medoids";

type Body = {
  cluster: number
}

const handler = async (req: NextApiRequest, res: NextApiResponse) => {
  const body = JSON.parse(req.body as string) as Body

  const client = new MongoClient(env.DB_URL)
  await client.connect()
  const db = client.db("wulrusso")
  const collection = db.collection("user")
  const dbRes = await collection.find().toArray()
  // todo: parse cluster data
  
  const clusterer = Clusterer.getInstance(dbRes, body.cluster);
  const clusteredData = clusterer.getClusteredData();

  console.log(clusteredData)
  // todo: add cluster data to db



  res.status(200).json(dbRes)
}

export default handler