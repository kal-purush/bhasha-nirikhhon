package com.futureworkshops.codetest.android.data.persistence;

import android.arch.persistence.room.ColumnInfo;
import android.arch.persistence.room.Entity;
import android.arch.persistence.room.PrimaryKey;

import com.futureworkshops.codetest.android.domain.model.BreedStats;

@Entity
public class StatsEntity {
    @PrimaryKey
    public long id;
    @ColumnInfo
    public int adaptability;
    @ColumnInfo
    public int friendliness;
    @ColumnInfo
    public int grooming;
    @ColumnInfo
    public int trainability;
    @ColumnInfo
    public int exercise_needs;

    public StatsEntity(long id, int adaptability, int friendliness, int grooming, int trainability, int exercise_needs) {
        this.id = id;
        this.adaptability = adaptability;
        this.friendliness = friendliness;
        this.grooming = grooming;
        this.trainability = trainability;
        this.exercise_needs = exercise_needs;
    }

    public StatsEntity(long id, BreedStats breedStats) {
        this.id = id;
        adaptability = breedStats.getAdaptability();
        friendliness = breedStats.getFriendliness();
        grooming = breedStats.getGrooming();
        trainability = breedStats.getTrainability();
        exercise_needs = breedStats.getExercise_needs();
    }

    public BreedStats toBreedStats() {
        return new BreedStats(adaptability,
                friendliness, grooming,
                trainability,
                exercise_needs);
    }
}