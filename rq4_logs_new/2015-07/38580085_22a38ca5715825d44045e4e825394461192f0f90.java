package com.primankaden.stay63.bl;

import com.primankaden.stay63.entities.AbsPoint;
import com.primankaden.stay63.entities.ClusterPoint;
import com.primankaden.stay63.entities.FullStop;
import com.primankaden.stay63.entities.marker.AbsMarker;
import com.primankaden.stay63.entities.marker.ClusterMarker;
import com.primankaden.stay63.entities.marker.StopMarker;
import com.primankaden.stay63.entities.marker.UserMarker;

import java.util.ArrayList;
import java.util.List;

public class MarkerBusinessLogic {
    private static final int CLUSTER_WIDTH = 5;
    private static final int CLUSTER_HEIGHT = 6;
    private static MarkerBusinessLogic instance;

    private MarkerBusinessLogic() {
    }

    public static MarkerBusinessLogic getInstance() {
        if (instance == null) {
            instance = new MarkerBusinessLogic();
        }
        return instance;
    }

    public List<AbsMarker> getPointsBetweenCords(double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
        List<FullStop> list = StopBusinessLogic.getInstance().getBetweenCoords(minLatitude, maxLatitude, minLongitude, maxLongitude);
        return new ClusterMatrix(list, minLatitude, maxLatitude, minLongitude, maxLongitude).asMarkerList();
    }

    public AbsMarker getUserMarker() {
        return new UserMarker(GeoBusinessLogic.getInstance().getCurrentPoint());
    }

    private class ClusterMatrix {
        ClusterMatrix(List<? extends AbsPoint> pointsBefore, double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
            double latRange = maxLatitude - minLatitude;
            double longRange = maxLongitude - minLongitude;
            for (AbsPoint p : pointsBefore) {
                int column = (int) ((p.getLongitude() - minLongitude) / (longRange / CLUSTER_WIDTH));
                int line = (int) ((p.getLatitude() - minLatitude) / (latRange / CLUSTER_HEIGHT));
                if (points[line][column] == null) {
                    points[line][column] = new ClusterPoint();
                }
                points[line][column].add(p);
            }
        }

        ClusterPoint points[][] = new ClusterPoint[CLUSTER_HEIGHT][CLUSTER_WIDTH];

        public List<AbsMarker> asMarkerList() {
            List<AbsMarker> result = new ArrayList<>();
            for (ClusterPoint[] ps : points) {
                for (ClusterPoint p : ps) {
                    if (p == null || p.isEmpty()) {
                        continue;
                    }
                    if (p.size() < 2) {
                        result.add(new StopMarker(p.get(0)));
                    } else {
                        result.add(new ClusterMarker(p));
                    }
                }
            }
            return result;
        }
    }
}