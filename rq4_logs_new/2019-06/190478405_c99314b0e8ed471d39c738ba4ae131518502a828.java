package com.github.code31415926535.engine.primitives;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SectorRenderDetails {
    private final Sector sector;
    private final Sector parent;
    private final int xStart, xEnd;

    private String getParentName() {
        if (parent == null) {
            return "null";
        }

        return parent.getName();
    }

    public String toString() {
        return "{" + sector.getName() + "-" + getParentName() + " [" + xStart + "," + xEnd + "] }";
    }
}