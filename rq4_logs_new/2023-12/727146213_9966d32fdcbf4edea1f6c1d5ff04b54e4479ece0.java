package com.example.flink;

import com.example.flink.data.AntennaData;
import com.example.flink.data.BeamData;
import com.example.flink.data.ChannelAntennaData;
import com.example.flink.data.ChannelBeamData;
import com.example.flink.data.ChannelData;
import com.example.flink.data.CoefficientData;
import com.example.flink.operation.BeamFormingWindowFunction;
import com.example.flink.operation.ChannelDataParser;
import com.example.flink.operation.ChannelDataUnitSplitter;
import com.example.flink.operation.ChannelMerge;
import com.example.flink.operation.GroupBeamOperator;
import com.example.flink.sink.JsonLogSink;
import com.example.flink.source.FPGASource;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CosmicAntennaApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(CosmicAntennaApp.class);

  public static void main(String[] args) throws Exception {
    // TODO separate coefficient matrix generation from main function
    // generate random coefficient matrix
    Path tempFilePath = generateCoefficientMatrix();
    // read configuration from environment variables
    List<OutputTag<ChannelBeamData>> outputTagList =
        IntStream.range(0, 3)
            .boxed()
            .map(
                index -> {
                  return new OutputTag<ChannelBeamData>(
                      RandomStringUtils.randomAlphabetic(10) + index) {};
                })
            .collect(Collectors.toList());
    Configuration configuration = CosmicAntennaConf.ConfigurationBuilder.build();
    int timeSampleSize = configuration.getInteger(CosmicAntennaConf.TIME_SAMPLE_SIZE);
    int timeSampleUnitSize = configuration.getInteger(CosmicAntennaConf.TIME_SAMPLE_UNIT_SIZE);
    int channelSize = configuration.getInteger(CosmicAntennaConf.CHANNEL_SIZE);
    int beamSize = configuration.getInteger(CosmicAntennaConf.BEAM_SIZE);
    int antennaSize = configuration.getInteger(CosmicAntennaConf.ANTENNA_SIZE);
    int beamFormingWindowSize =
        configuration.getInteger(CosmicAntennaConf.BEAM_FORMING_WINDOW_SIZE);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ExecutionConfig executionConfig = env.getConfig();
    executionConfig.setGlobalJobParameters(configuration);
    executionConfig.setAutoWatermarkInterval(20000L);
    GroupBeamOperator groupBeamOperator =
        GroupBeamOperator.builder()
            .algorithmNameList(
                IntStream.range(0, 3)
                    .mapToObj(index -> RandomStringUtils.randomAlphabetic(8))
                    .collect(Collectors.toList()))
            .build();
    SingleOutputStreamOperator<BeamData> beamDataStream =
        env.addSource(new FPGASource())
            // TODO source parallelism set with CosmicAntennaConf
            .setParallelism(2)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<AntennaData>forBoundedOutOfOrderness(
                        // TODO configure duration
                        Duration.ofSeconds(timeSampleSize))
                    // TODO package counter should multiply with iteration count: redis counter++?
                    .withTimestampAssigner(
                        (antennaData, timestamp) ->
                            antennaData.getPackageCounter()
                                / (timeSampleSize / timeSampleUnitSize)))
            .flatMap(
                ChannelDataParser.builder()
                    .channelSize(configuration.getInteger(CosmicAntennaConf.CHANNEL_SIZE))
                    .timeSampleSize(configuration.getInteger(CosmicAntennaConf.TIME_SAMPLE_SIZE))
                    .build())
            .keyBy(ChannelAntennaData::getChannelId)
            .window(
                SlidingEventTimeWindows.of(
                    // TODO configure window size
                    Time.milliseconds(timeSampleSize), Time.milliseconds(timeSampleSize)))
            .aggregate(
                ChannelMerge.builder()
                    .timeSampleSize(timeSampleSize)
                    .antennaSize(antennaSize)
                    .build())
            .flatMap(
                ChannelDataUnitSplitter.builder()
                    .timeSampleSize(timeSampleSize)
                    .timeSampleUnitSize(timeSampleUnitSize)
                    .build())
            .keyBy((KeySelector<ChannelData, Integer>) ChannelData::getChannelId)
            .window(
                SlidingEventTimeWindows.of(
                    Time.milliseconds(timeSampleSize), Time.milliseconds(timeSampleSize)))
            .apply(
                BeamFormingWindowFunction.builder()
                    .channelSize(channelSize)
                    .beamSize(beamSize)
                    .antennaSize(antennaSize)
                    .timeSampleUnitSize(timeSampleUnitSize)
                    .beamFormingWindowSize(beamFormingWindowSize)
                    // TODO add coefficient data list
                    .coefficientDataList(null)
                    .build())
            .keyBy((KeySelector<ChannelBeamData, Integer>) ChannelBeamData::getBeamId)
            .window(
                // TODO configure window size
                SlidingEventTimeWindows.of(
                    Time.milliseconds(timeSampleSize), Time.milliseconds(timeSampleSize)))
            .process(groupBeamOperator);
    for (OutputTag<BeamData> outputTag : groupBeamOperator.getOutputTagList()) {
      beamDataStream
          .getSideOutput(outputTag)
          .addSink(JsonLogSink.<BeamData>builder().build())
          .setParallelism(1)
          .name(outputTag.toString() + "_sink");
    }
    env.execute("transform example of sensor reading");
    // TODO remove file creation and deletion from main function
    FileUtils.deleteFileOrDirectory(tempFilePath.toFile());
    LOGGER.info("deleted coefficient data temp file");
  }

  private static Path generateCoefficientMatrix() throws IOException {
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    Path tempFile = Files.createTempFile("coefficient-", ".json");
    LOGGER.info("coefficient data saved in -> {}", tempFile.toString());
    Random random = new Random(666);

    List<CoefficientData> coefficientDataList =
        IntStream.range(0, 1000)
            .boxed()
            .map(
                channelId -> {
                  byte[] realArray = new byte[224 * 180];
                  byte[] imaginaryArray = new byte[224 * 180];
                  random.nextBytes(realArray);
                  random.nextBytes(imaginaryArray);
                  return CoefficientData.builder()
                      .channelId(channelId)
                      .realArray(realArray)
                      .imaginaryArray(imaginaryArray)
                      .build();
                })
            .collect(Collectors.toList());

    FileUtils.writeFileUtf8(
        tempFile.toFile(), OBJECT_MAPPER.writeValueAsString(coefficientDataList));
    return tempFile;
  }
}