package dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@AllArgsConstructor
@Data
@Builder
public class WorkoutCalculator {

    String selectEvent;
    String hoursIntensity;
    String minutesIntensity;
    String secondsIntensity;
}