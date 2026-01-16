import axios from "axios";
import { NextRequest, NextResponse } from "next/server";
import { getServerSession } from "next-auth/next";
import { authOptions } from "../auth/[...nextauth]/route";

export async function POST(req: NextRequest, res: NextResponse) {
  const session: any = await getServerSession(authOptions);
  const user_id = session?.user?.user?.user?.id;
  const formData = await req.formData();
  const dataBody = formData.get("data") as any;
  const parsedBody = JSON.parse(dataBody);

  try {
    const address = await axios.put(
      `${process.env.NEXT_PUBLIC_API_URL}/addresses`,
      {
        user_id: user_id,
        restaurant_id: parsedBody.restaurant_id,
        house_no: parsedBody.house_no,
        street1: parsedBody.street1,
        street2: parsedBody.street2,
        city: parsedBody.city,
        state: parsedBody.state,
        country: parsedBody.country,
        postal_code: parsedBody.postal_code,
        latitude: parsedBody.latitude,
        longitude: parsedBody.longitude,
        type: parsedBody.type,
      },
      {
        headers: {
          "Content-Type": "application/json",
        },
      }
    );

    return new NextResponse(JSON.stringify("Created successfully..."));
  } catch (error) {
    console.error("Error creating address...", error);

    return new NextResponse(JSON.stringify({ status: 404 }));
  }
}