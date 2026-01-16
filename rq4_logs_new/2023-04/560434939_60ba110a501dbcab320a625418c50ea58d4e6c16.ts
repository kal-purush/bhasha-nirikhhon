import type { NextApiRequest, NextApiResponse } from "next";
import database from "../../library/database";

const handler = async (request: NextApiRequest, response: NextApiResponse) => {
  if (request.method !== "PUT") {
    return response.status(405).send({ message: "Invalid Method" });
  }
  const body = JSON.parse(request.body);
  try {
    const household = await database.household.update({
      where: {
        id_userId: {
          id: body.id,
          userId: body.userId,
        },
      },
      data: {
        head: true,
      },
    });
    return response.status(200).send(household);
  } catch (error) {
    return response.status(500).send({ message: "Internal server error" });
  }
};

export default handler;