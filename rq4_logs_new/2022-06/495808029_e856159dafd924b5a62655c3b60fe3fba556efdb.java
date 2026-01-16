package dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@AllArgsConstructor
@Data
@Builder
public class WorkoutCalculators {

    String selectEvent;
    String hoursIntensity;
    String minutesIntensity;
    String secondsIntensity;

    String distanceHansons;
    String distance;
    String distanceType;
    String hoursHansons;
    String minutesHansons;
    String secondsHansons;
    String temperature;
    String temperatureType;
    String windSpeed;
    String speedType;
    String humidity;

    String distanceMillan;
    String hoursMillan;
    String minutesMillan;
    String secondsMillan;

    String raceDistance;
    String hoursTinman;
    String minutesTinman;
    String secondsTinman;
    String gender;

    String CP;
    String RWC;
    String shortTestMinutes;
    String shortTestSeconds;
    String shortTestAvgPower;
    String longTestMinutes;
    String longTestSeconds;
    String longTestAvgPower;
    String raceHours;
    String raceMinutes1;
    String raceSeconds1;
    String raceAvgPower1;
    String raceMinutes2;
    String raceSeconds2;
    String raceAvgPower2;
}