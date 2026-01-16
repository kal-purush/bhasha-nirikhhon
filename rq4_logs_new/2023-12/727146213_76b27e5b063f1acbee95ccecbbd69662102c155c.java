package com.example.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class CosmicAntennaConf {
    public static final ConfigOption<Integer> TIME_SAMPLE_SIZE = ConfigOptions
            .key("cosmic.antenna.timeSampleSize")
            .intType()
            .defaultValue(2048);
    public static final ConfigOption<Integer> TIME_SAMPLE_UNIT_SIZE = ConfigOptions
            .key("cosmic.antenna.timeSampleUnitSize")
            .intType()
            .defaultValue(64);
    public static final ConfigOption<Integer> ANTENNA_SIZE = ConfigOptions
            .key("cosmic.antenna.antennaSize")
            .intType()
            .defaultValue(224);
    public static final ConfigOption<Long> START_COUNTER = ConfigOptions
            .key("cosmic.antenna.startCounter")
            .longType()
            .defaultValue(0L);
    public static final ConfigOption<Long> SLEEP_TIME_INTERVAL = ConfigOptions
            .key("cosmic.antenna.sleepTimeInterval")
            .longType()
            .defaultValue(1000L);
}