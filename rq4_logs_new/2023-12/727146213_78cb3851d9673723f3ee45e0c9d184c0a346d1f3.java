package com.example.flink.operation;

import com.example.flink.data.BeamData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupByBeam extends ProcessWindowFunction<BeamData, BeamData, Integer, TimeWindow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupByBeam.class);

    @Override
    public void open(Configuration configuration) throws Exception {

    }

    @Override
    public void process(Integer integer,
            ProcessWindowFunction<BeamData, BeamData, Integer, TimeWindow>.Context context,
            Iterable<BeamData> elements, Collector<BeamData> out) throws Exception {

        for (BeamData beamData : elements) {
            out.collect(beamData);
        }
    }
}