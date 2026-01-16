import { type NextApiRequest, type NextApiResponse } from "next"
import { env } from "../../env.mjs"
import { MongoClient } from "mongodb"

type Body = {
  name: string
  type: "user" | "team"
  point: number
}

const handler = async (req: NextApiRequest, res: NextApiResponse) => {
  const client = new MongoClient(env.DB_URL)

  // to do: handle exception
  const body = JSON.parse(req.body as string) as Body
  

  await client.connect()
  const db = client.db("wulrusso")
  const collection = db.collection("quest")
  // to do: don't store password in plain text 
  // will use hmac + salt
  const dbRes = await collection.insertOne(body)


  res.status(200).json(dbRes)
}

export default handler