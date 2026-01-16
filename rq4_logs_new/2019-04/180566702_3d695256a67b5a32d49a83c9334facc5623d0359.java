package com.healthsdk.data;

import java.util.Objects;

public class DataTimeRange {
    public final long startTime;
    public final long endTime;

    public DataTimeRange(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataTimeRange that = (DataTimeRange) o;
        return startTime == that.startTime &&
                endTime == that.endTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, endTime);
    }

    @Override
    public String toString() {
        return "DataTimeRange{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}