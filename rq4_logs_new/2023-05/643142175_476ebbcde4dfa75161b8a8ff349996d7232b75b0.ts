import { type NextApiRequest, type NextApiResponse } from "next"
import { env } from "../../env.mjs"
import { MongoClient } from "mongodb"

interface Body {
  id: string
}

const handler = async (req: NextApiRequest, res: NextApiResponse) => {
  const body = JSON.parse(req.body as string) as Body
  const client = new MongoClient(env.DB_URL)

  await client.connect()
  const db = client.db("wulrusso")
  const collection = db.collection("account")

  const dbRes = await collection.findOne({ userId: body.id })

  res.status(200).json(dbRes)
}

export default handler