import type { NextApiRequest, NextApiResponse } from "next";
import database from "../../library/database";

const handler = async (request: NextApiRequest, response: NextApiResponse) => {
  if (request.method !== "PUT") {
    return response.status(405).send({ message: "Invalid Method" });
  }
  const body = JSON.parse(request.body);
  try {
    const user = await database.user.update({
      where: {
        id: body.id,
      },
      data: {
        email: body.email,
        givenName: body.givenName,
        middleName: body.middleName,
        familyName: body.familyName,
        gender: body.gender,
        birthdate: body.birthdate,
        birthplace: body.birthplace,
        phone: body.phone,
        occupation: body.occupation,
        homeowner: body.homeowner,
        voter: body.voter,
        fullAddress: body.fullAddress
      },
    });
    return response.status(201).send(user);
  } catch (error) {
    return response.status(505).send({ message: "Internal server error" });
  }
};

export default handler;