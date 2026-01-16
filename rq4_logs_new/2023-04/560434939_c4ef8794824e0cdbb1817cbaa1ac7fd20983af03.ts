import type { NextApiRequest, NextApiResponse } from "next";
import database from "../../library/database";

const handler = async (request: NextApiRequest, response: NextApiResponse) => {
  if (request.method !== "PUT") {
    return response.status(405).send({ message: "Invalid Method" });
  }
  const body = JSON.parse(request.body);
  try {
    const document = await database.user.update({
      where: {
        email: body.email,
      },
      data: {
        password: body.password,
      },
    });
    return response.status(201).send(document);
  } catch (error) {
    return response.status(505).send({ message: "Internal server error" });
  }
};

export default handler;