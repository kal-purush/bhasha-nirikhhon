package hu.oe.nik.szfmv.automatedcar.virtualfunctionbus.packets.Camera;

import hu.oe.nik.szfmv.automatedcar.model.objects.RoadSign;

import java.awt.*;

public class CameraPacket implements ReadOnlyCameraPacket {

    private static CameraPacket cameraPacket = null;
    private RoadSign roadSign;
    private Point[] points;


    public static CameraPacket getPacket() {
        if (cameraPacket == null) {
            cameraPacket = new CameraPacket();
        }
        return cameraPacket;
    }

    @Override
    public RoadSign getRoadSign() {
        return roadSign;
    }

    @Override
    public Point[] getTrianglePoints() {
        return points;
    }

    public  void setRoadSign(RoadSign roadSign) {
        this.roadSign = roadSign;
    }

    public  void setTriangle(Point[] points) {
        this.points = points;
    }
}