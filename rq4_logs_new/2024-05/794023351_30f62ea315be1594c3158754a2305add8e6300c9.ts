import { dbRequest } from "../config/dbRequest";

export async function GET(request: Request) {
  try {
    const data = await dbRequest("cardData", "find");

    return Response.json(data);
  } catch (error) {
    console.log(error);
    throw new Error("aldaa");
  }
}

export async function POST(request: Request) {
  const body = await request.json();
  const { cvc, region, nameOnCard, cardNumber, date } = body;
  try {
    const data = await dbRequest("cardData", "insertOne", {
      document: {
        cvc: cvc,
        nameOnCard: nameOnCard,
        date: date,
        cardNumber: cardNumber,
        region: region,
      },
    });

    return Response.json(data);
  } catch (error) {
    console.log(error);

    throw new Error("aldaa");
  }
}