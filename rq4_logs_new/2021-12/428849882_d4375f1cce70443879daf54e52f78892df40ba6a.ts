import { NextApiRequest, NextApiResponse } from "next";
import { getDocs, query, collection } from "firebase/firestore";
import { db } from "../../firebase";

export default async function getid(req: NextApiRequest, res: NextApiResponse) {
  const { method } = req as any;

  switch (method) {
    case "GET":
      const q = query(collection(db, "items"));

      const querySnapshot = await getDocs(q);
      const items = querySnapshot.docs.map((doc) => ({
        id: doc.id,
        ...doc.data(),
      }));

      res.status(200).json(items);
      break;
    default:
      res.setHeader("Allow", ["GET"]);
      res.status(405).end(`Method ${method} Not Allowed`);
  }
}