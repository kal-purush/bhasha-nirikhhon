package com.smu.team_andeu.data;

import androidx.room.Entity;
import androidx.room.PrimaryKey;

@Entity
public class RoutineName {
    @PrimaryKey
    int routineId;
    String r_name;

    public RoutineName(int routineId, String r_name) {
        this.routineId = routineId;
        this.r_name = r_name;
    }
}