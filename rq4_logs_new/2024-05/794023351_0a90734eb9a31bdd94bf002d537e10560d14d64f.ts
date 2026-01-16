import dotenv from "dotenv";
import { NextApiRequest } from "next";

dotenv.config();
export async function GET(req: NextApiRequest) {
  const { start, end, date } = await req.body.json();
  try {
    const amadeusResponse = await fetch(
      `https://test.api.amadeus.com/v2/shopping/flight-offers?originLocationCode=${start}&destinationLocationCode=${end}&departureDate=${date}`
    );
    const amadeusData = await amadeusResponse.json();
    Response.json(amadeusData);
  } catch (error) {
    console.error(error);
    Response.json({ error: "Internal Server Error" });
  }
}