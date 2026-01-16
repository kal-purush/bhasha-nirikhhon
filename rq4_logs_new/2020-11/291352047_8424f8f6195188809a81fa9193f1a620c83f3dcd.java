package com.operationcodify.cavoid.database;

import androidx.annotation.NonNull;
import androidx.room.ColumnInfo;
import androidx.room.Entity;

import org.joda.time.Instant;
import org.joda.time.LocalDate;

@Entity
public class PastLocationWithActiveCases {

    @NonNull
    @ColumnInfo(name = "date")
    public LocalDate date;

    @NonNull
    @ColumnInfo(name = "fips")
    public String fips;

    @ColumnInfo(name="county_name")
    public String countyName;

    @ColumnInfo(name="active_cases")
    public int activeCases;

    @ColumnInfo(name="report_date")
    public LocalDate reportDate;

    public long timestamp;
}