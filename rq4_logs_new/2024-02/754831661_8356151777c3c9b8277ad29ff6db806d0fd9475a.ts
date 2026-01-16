import { NextApiRequest, NextApiResponse } from "next";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const { coordinates } = req.query;

  try {
    // Make a request to the Mapbox Isochrone API
    const response = await fetch(
      `https://api.mapbox.com/isochrone/v1/mapbox/driving/${coordinates}?contours_minutes=10,16,25&contours_colors=89623e,24e813,4282f4&polygons=true&access_token=${process.env.NEXT_PUBLIC_MAPBOX_API_KEY}`
    );
    
    if (!response.ok) {
      throw new Error("Failed to generate isochrones");
    }

    const data = await response.json();
    const isochrones = data;

    res.status(200).json({ isochrones });
  } catch (error) {
    console.error("Error generating isochrones:", error);
    res.status(500).json({ error: "Failed to generate isochrones" });
  }
}