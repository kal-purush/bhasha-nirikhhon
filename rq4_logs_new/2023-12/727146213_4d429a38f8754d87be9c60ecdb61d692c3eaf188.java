package com.example.flink.util;

import com.example.flink.CosmicAntennaApp;
import com.example.flink.data.CoefficientData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoefficientDataUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(CosmicAntennaApp.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String FILE_PREFIX = "coefficient-";

  public static Path generateCoefficientDataFile(Integer channelSize) throws IOException {
    Path tempDirectory = Files.createTempDirectory("cosmic-antenna");

    LOGGER.info("coefficient data saved in -> {}", tempDirectory);
    Random random = new Random(666);

    IntStream.range(0, channelSize)
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
        .forEach(
            item -> {
              try {
                Path tempFile =
                    Files.createTempFile(
                        tempDirectory, FILE_PREFIX + item.getChannelId() + "-", ".json");
                FileUtils.writeFileUtf8(tempFile.toFile(), OBJECT_MAPPER.writeValueAsString(item));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

    return tempDirectory;
  }

  public static List<CoefficientData> retrieveCoefficientDataList(String dirPath)
      throws IOException {
    Collection<Path> dataFiles =
        FileUtils.listFilesInDirectory(
            Path.of(dirPath), path -> path.getFileName().toString().startsWith(FILE_PREFIX));
    return dataFiles.stream()
        .map(
            file -> {
              try {
                return OBJECT_MAPPER.readValue(
                    file.toFile(), new TypeReference<CoefficientData>() {});
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }
}