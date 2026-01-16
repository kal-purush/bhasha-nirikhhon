/*
 * Copyright (c) 2014-2019 Aurélien Broszniowski
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rainfall.store.client;

import io.rainfall.store.core.ChangeReport;
import io.rainfall.store.core.ClientJob;
import io.rainfall.store.core.OperationOutput;
import io.rainfall.store.core.StatsLog;
import io.rainfall.store.core.TestRun;
import io.rainfall.store.data.CompressionService;
import io.rainfall.store.data.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.qa.angela.client.filesystem.TransportableFile;
import com.terracottatech.qa.angela.common.clientconfig.ClientId;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.lang.System.getenv;
import static java.util.Optional.ofNullable;

public class DefaultStoreClientService implements StoreClientService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStoreClientService.class);
  private static final Set<String> outputFileExtensions = OperationOutput.allFormats();

  private final StoreClient writer;
  private final CompressionService compressionService;

  DefaultStoreClientService(StoreClient writer, CompressionService compressionService) {
    this.writer = writer;
    this.compressionService = compressionService;
  }

  @Override
  public long addRun(String caseName, String className, String terracottaVersion) {
    TestRun run = TestRun.builder()
        .checksum(getPreviousSuccessfulCommit())
        .className(className)
        .version(terracottaVersion)
        .build();
    try {
      long runId = writer.addRun(caseName, run);
      LOGGER.info("Test run created: ID={}, run={}, test case = {}.",
          new Object[] { runId, run, caseName });
      return runId;
    } catch (RuntimeException e) {
      LOGGER.error("Failed to add a run for the test case name '{}'; " +
                   "check that the test case exists.", caseName);
      throw e;
    }
  }

  @Override
  public boolean setStatus(long runId, TestRun.Status status) {
    try {
      boolean success = writer.setStatus(runId, status);
      if (success) {
        LOGGER.info("Status of run {} set to {}.", runId, status);
      } else {
        LOGGER.error("Run ID not found: {}.", runId);
      }
      return success;
    } catch (RuntimeException e) {
      LOGGER.error("Failed to update the status for the run '{}' to {}: {}",
          new Object[] { runId, status, e.getMessage() });
      throw e;
    }
  }

  static String getPreviousSuccessfulCommit() {
    String envVar = "GIT_PREVIOUS_SUCCESSFUL_COMMIT";
    return ofNullable(getenv(envVar))
        .orElseGet(() -> {
          LOGGER.warn(envVar +
                      " environment variable not defined.");
          return "";
        });
  }

  @Override
  public long addClientJob(long runId, int clientNumber, ClientId clientId, List<String> details, String outputPath) {
    try {
      ClientJob clientJob = ClientJob.builder()
          .host(clientId.getHostname())
          .clientNumber(clientNumber)
          .symbolicName(clientId.getSymbolicName().getSymbolicName())
          .details(String.join("\n", details))
          .build();
      long jobId = writer.addClientJob(runId, clientJob);
      LOGGER.info("Client job created: ID={}, job={}, run ID = {}.",
          new Object[] { jobId, clientJob, runId });
      uploadOutputs(jobId, outputPath);
      return jobId;
    } catch (Exception e) {
      LOGGER.error("Failed to add client job to run {}: {}.", runId, e.getMessage());
      throw new IllegalStateException("Failed to add client job.", e);
    }
  }

  private void uploadOutputs(long jobId, String outputPath) {
    File[] files = new File(outputPath).listFiles(
        f -> !f.isDirectory() && f.getName().contains("."));
    if (files == null) {
      String msg = String.format("Failed to locate the output for client job %d in %s.",
          jobId, outputPath);
      LOGGER.error(msg);
      throw new IllegalStateException(msg);
    }
    Stream.of(files).forEach(file -> {
      String[] toks = file.getName().split("\\.", 2);
      String extension = toks[1];
      if (outputFileExtensions.contains(extension)) {
        try {
          byte[] data = Files.readAllBytes(file.toPath());
          Payload payload = compressionService.compress(data);
          OperationOutput output = OperationOutput.builder()
              .operation(toks[0])
              .format(extension)
              .payload(payload)
              .build();
          long outputId = writer.addOutput(jobId, output);
          LOGGER.info("Output file {} uploaded: ID={}, job ID = {}.",
              new Object[] { file, outputId, jobId });
        } catch (IOException e) {
          LOGGER.error("Output upload failed for client job {}, file is {}.",
              jobId, file.getPath());
          throw new IllegalStateException("Output upload failed.", e);
        }
      }
    });
  }

  @Override
  public long addMetrics(long runId, String host, TransportableFile transFile) {
    try {
      String fileNameBase = transFile.getName()
          .replaceAll("\\.log$", "");
      byte[] content = transFile.getContent();
      Payload payload = compressionService.compress(content);
      StatsLog statsLog = StatsLog.builder()
          .host(host)
          .type(fileNameBase)
          .payload(payload)
          .build();
      long logId = writer.addStatsLog(runId, statsLog);
      LOGGER.info("Stats log created: ID={}, log = {}, run ID = {}.",
          new Object[] { logId, statsLog, runId });
      return logId;
    } catch (IOException e) {
      LOGGER.error("Failed to add stats log to run {}: {}.", runId, e.getMessage());
      throw new IllegalStateException("Failed to add stats log.", e);
    }
  }

  @Override
  public ChangeReport checkRegression(long runId, double threshold) {
    try {
      return writer.checkRegression(runId, threshold);
    } catch (RuntimeException e) {
      LOGGER.error("Failed to check regression for run {} " +
                   "with threshold {}: {}.", new Object[] { runId, threshold, e.getMessage() });
      throw e;
    }
  }
}