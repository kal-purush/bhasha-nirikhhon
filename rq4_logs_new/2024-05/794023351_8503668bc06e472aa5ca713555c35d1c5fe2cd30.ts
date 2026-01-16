import { dbRequest } from "../config/dbRequest";

export async function GET(request: Request) {
  try {
    const { documents } = await dbRequest("admin", "find");

    return Response.json(documents);
  } catch (error) {
    console.log(error);
    throw new Error("aldaa");
  }
}

export async function POST(request: Request) {
  const body = await request.json();
  const {
    flightNumber,
    airline,
    aircraft,
    depCountry,
    depCity,
    depTime,
    arrCountry,
    arrCity,
    arrTime,
    price,
    duration,
  } = body;

  try {
    const data = await dbRequest("admin", "insertOne", {
      document: {
        flightNumber: flightNumber,
        airline: airline,
        aircraft: aircraft,
        depCountry: depCountry,
        depCity: depCity,
        depTime: depTime,
        arrCountry: arrCountry,
        arrCity: arrCity,
        arrTime: arrTime,
        price: price,
        duration: duration,
      },
    });

    return Response.json(data);
  } catch (error) {
    console.log(error);

    throw new Error("aldaa");
  }
}