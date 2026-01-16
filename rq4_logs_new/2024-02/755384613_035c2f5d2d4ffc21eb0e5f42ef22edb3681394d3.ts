import { NextResponse, type NextRequest } from "next/server";

export const dynamic = "force-dynamic";

export const dynamicParams = true; // true | false,

export async function GET(req: NextRequest) {
  try {
    const searchParams = req.nextUrl.searchParams;
    const url = searchParams.get("url");

    if (!url) {
      return NextResponse.json({ error: "URL is required" }, { status: 422 });
    }

    // Fetch the Google Form response
    // const response = await fetch(url as string);

    const response = await fetch(url as string);

    // Check if the response is successful
    if (!response.ok) {
      return NextResponse.json(
        { error: "Failed to fetch Google Form response", data: response },
        { status: 422 }
      );
    }

    let pure_url;
    if (response.redirected) {
      pure_url = response.url; // This is the real redirect URL
    } else {
      const locationHeader = response.headers.get("location");
      if (locationHeader) {
        return locationHeader; // This is the real redirect URL
      } else {
        pure_url = url; // This is the real redirect URL
      }
    }

    return NextResponse.json({
      message: "Successfully retrieved Google Form",
      data: {
        url: url,
        pure_url: pure_url,
      },
    });
  } catch (error) {
    console.error("Error fetching Google Form response:", error);
    return NextResponse.error();
  }
}