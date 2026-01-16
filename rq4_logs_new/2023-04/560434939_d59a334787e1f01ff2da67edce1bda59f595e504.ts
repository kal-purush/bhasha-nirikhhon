import type { NextApiRequest, NextApiResponse } from "next";
import database from "../../library/database";

const handler = async (request: NextApiRequest, response: NextApiResponse) => {
  if (request.method !== "DELETE") {
    return response.status(405).send({ message: "Invalid Method" });
  }
  const body = JSON.parse(request.body);
  try {
    const signatory = await database.signatory.delete({
      where: {
        id: body.id,
      },
    });
    return response.status(200).send({ message: "Signatory deleted" });
  } catch (error) {
    return response.status(500).send({ message: "Internal server error" });
  }
};

export default handler;