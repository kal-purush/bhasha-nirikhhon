package com.dailytracker;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;

@Getter
public class DailyActivities {
    private final LocalDate date;
    @Setter
    private boolean herbRunCompleted;
    @Setter
    private boolean birdHouseRunCompleted;
    @Setter
    private boolean dailyBattlestavesCollected;
    @Setter
    private boolean dailyEssenceCollected;
    @Setter
    private int dailyFarmingContractCompleted;

    public DailyActivities() {
        this.date = LocalDate.now();
    }

    // Getters and setters for all fields

}