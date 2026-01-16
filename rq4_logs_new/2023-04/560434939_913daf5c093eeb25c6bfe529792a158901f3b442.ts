import type { NextApiRequest, NextApiResponse } from "next";
import database from "../../library/database";

const handler = async (request: NextApiRequest, response: NextApiResponse) => {
  if (request.method !== "POST") {
    return response.status(405).send({ message: "Invalid Method" });
  }

  const {
    userId,
    givenName,
    middleName,
    familyName,
    gender,
    birthdate,
    birthplace,
    phone,
    occupation,
    relationship,
  } = JSON.parse(request.body);

  try {
    const user = await database.user.findUnique({ where: { id: userId } });
    if (!user) {
      return response.status(404).send({ message: "User not found" });
    }

    const household = await database.household.create({
      data: {
        givenName,
        middleName,
        familyName,
        gender,
        birthdate,
        birthplace,
        phone,
        occupation,
        relationship,
        user: {
          connect: {
            id: user.id,
          },
        },
      },
    });

    return response.status(201).send(household);
  } catch (error) {
    console.error(error);
    return response.status(500).send({ message: "Internal server error" });
  }
};

export default handler;