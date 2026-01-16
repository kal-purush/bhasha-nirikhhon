import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/prisma";
import * as turf from "@turf/turf";

// Haversine distance in meters between two [lat, lng] points
function haversine([lat1, lng1]: [number, number], [lat2, lng2]: [number, number]) {
  const toRad = (x: number) => (x * Math.PI) / 180;
  const R = 6371000; // Earth radius in meters
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLng / 2) * Math.sin(dLng / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

export async function GET(req: NextRequest) {
  const tracks = await prisma.activityTrack.findMany();
  const features = tracks
    .filter(track => Array.isArray(track.track))
    .map(track => {
      const latlngs = track.track as [number, number][];
      if (latlngs.length < 3) return null;
      const dist = haversine(latlngs[0], latlngs[latlngs.length - 1]);
      const isLoop = dist < 100; // 100 meters
      if (isLoop) {
        const points = turf.featureCollection(
          latlngs.map(([lat, lng]) => turf.point([lng, lat]))
        );
        let hull = turf.concave(points, { maxEdge: 0.3 });
        if (!hull) hull = turf.convex(points);
        if (hull && hull.geometry.type === "Polygon") {
          return {
            type: "Feature",
            geometry: hull.geometry,
            properties: { activityId: track.activityId, type: "polygon" },
          };
        }
      }
      return null;
    })
    .filter(Boolean);
  return NextResponse.json({
    type: "FeatureCollection",
    features,
  });
} 