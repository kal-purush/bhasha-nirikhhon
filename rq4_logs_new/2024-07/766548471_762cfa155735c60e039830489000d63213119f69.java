package com.example.swiftgathering_server.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@NoArgsConstructor
public class DrawingDto {
    private Long senderId;
    private String channelId;
    public StrokeState state;
    public StrokePath path;

    public DrawingDto(StrokeState state, StrokePath path) {
        this.state = state;
        this.path = path;
    }

    @Getter
    @NoArgsConstructor
    public static class StrokeState {
        public String color;
        public float width;
        public float alpha;
        public String blendMode;

        public StrokeState(String color, float width, float alpha, String blendMode) {
            this.color = color;
            this.width = width;
            this.alpha = alpha;
            this.blendMode = blendMode;
        }
    }

    @Getter
    @NoArgsConstructor
    public static class StrokePath {
        public List<Coordinate> coordinates;

        public StrokePath(List<Coordinate> coordinates) {
            this.coordinates = coordinates;
        }
    }

    @Getter
    @NoArgsConstructor
    public static class Coordinate {
        public double latitude;
        public double longitude;

        public Coordinate(double latitude, double longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }
}