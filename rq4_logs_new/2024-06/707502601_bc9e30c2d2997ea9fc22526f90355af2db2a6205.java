package com.bliss.blissapp.Model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
@Getter
@Setter
public class Location {
  private final double latitude;
  private final double longitude;

  public boolean withinDistance(Location location, double distance) {
    double lat1Rad = Math.toRadians(this.latitude);
    double lat2Rad = Math.toRadians(location.latitude);
    double lon1Rad = Math.toRadians(this.longitude);
    double lon2Rad = Math.toRadians(location.longitude);
    double EARTH_RADIUS = 6378.137;

    double x = (lon2Rad - lon1Rad) * Math.cos((lat1Rad + lat2Rad) / 2);
    double y = (lat2Rad - lat1Rad);
    double distanceApart = Math.sqrt(x * x + y * y) * EARTH_RADIUS;

    return distanceApart <= distance;
  }

}
