package helpers;

import tddmicroexercises.tirepressuremonitoringsystem.Alarm;
import tddmicroexercises.tirepressuremonitoringsystem.SafetyRange;
import tddmicroexercises.tirepressuremonitoringsystem.Sensor;

public class AlarmBuilder {
    private SafetyRange safetyRange;
    private Sensor sensor;

    public static AlarmBuilder anAlarm() {
        return new AlarmBuilder();
    }

    public AlarmBuilder usingSensor(Sensor sensor) {
        this.sensor = sensor;
        return this;
    }

    public AlarmBuilder andWithSafetyRange(double lowerThreshold, double higherThreshold) {
        this.safetyRange = new SafetyRange(lowerThreshold, higherThreshold);
        return this;
    }

    public Alarm build() {
        return new Alarm(sensor, safetyRange);
    }
}