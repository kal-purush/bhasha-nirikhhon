package com.example.distributed.consensus;

import com.example.distributed.consensus.data.LockFileContent;
import com.example.distributed.consensus.minio.MinioConnection;
import com.example.distributed.consensus.minio.MinioManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class Downloader {
    private static final Logger LOGGER = LoggerFactory.getLogger(Downloader.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final File WORKING_DIR = FileUtils.current();

    private static final Object lock = new ReentrantLock();

    private static final Integer timeout = 1000 * 60 * 2;

    public static void main(String[] args) throws Exception {

        String hostName = Optional.ofNullable(System.getenv("HOSTNAME")).orElse("ayayay");
        String endpoint = Optional.ofNullable(System.getenv("S3_ENDPOINT")).orElse("http://10.11.33.132:9000");
        String accessKey = Optional.ofNullable(System.getenv("S3_ACCESS_KEY")).orElse("minioadmin");
        String secretKey = Optional.ofNullable(System.getenv("S3_SECRET_KEY")).orElse("minioadmin");
        String bucket = Optional.ofNullable(System.getenv("DC_FILE_BUCKET")).orElse("filesystem");
        String filePathInS3 = Optional.ofNullable(System.getenv("DC_FILE_READ_PATH")).orElse("app.jar");
        String targetFileSavePath = Optional.ofNullable(System.getenv("DC_FILE_SAVE_PATH")).orElse(WORKING_DIR.getAbsolutePath());

        LOGGER.info("Retrieve hostname is {}", hostName);
        LOGGER.info("Download file will be saved in {}", targetFileSavePath);
        LOGGER.info("Downloader will connect minio[{}@{}/{}]", accessKey, secretKey, endpoint);
        LOGGER.info("Downloader trying to retrieve file[{}] at bucket:{}", filePathInS3, bucket);

        String fileFullName = FilenameUtils.getName(filePathInS3);
        String fileBaseName = FilenameUtils.getBaseName(filePathInS3);
        String fileLockName = fileBaseName + ".lock";
        String fileTmpName = fileBaseName + ".tmp";

        File targetFile = FileUtils.getFile(targetFileSavePath, fileFullName);
        File lockFile = FileUtils.getFile(targetFileSavePath, fileLockName);
        File tmpFile = FileUtils.getFile(targetFileSavePath, fileTmpName);

        LockFileContent lockFileContent = null;
        if (!targetFile.exists()) {

            do {
                if (lockFile.exists()) {
                    lockFileContent = OBJECT_MAPPER.readValue(
                            FileUtils.readFileToString(lockFile, StandardCharsets.UTF_8), new TypeReference<>() {
                            });
                    LOGGER.info("get lock file content -> {}", lockFileContent);
                } else {
                    lockFile = new File(targetFileSavePath, lockFile.getName());
                    lockFileContent = LockFileContent.builder()
                            .hostName(hostName)
                            .created(System.currentTimeMillis())
                            .build();
                    FileUtils.writeStringToFile(lockFile, OBJECT_MAPPER.writeValueAsString(lockFileContent),
                            StandardCharsets.UTF_8, false);
                    LOGGER.info("created a new lock file, containing content {}", lockFileContent);
                }

                synchronized (lock) {
                    while (!lockFileContent.getHostName().equals(hostName)) {
                        if (lockFileContent.getCreated() + timeout > System.currentTimeMillis()) {
                            Thread.sleep(3000L);
                            LOGGER.info("have been waiting 3 seconds, since different hostname");
                            if (targetFile.exists()){
                                LOGGER.info("during waiting, other thread had download file[{}], so I quit.", fileFullName);
                                FileUtils.deleteQuietly(lockFile);
                                FileUtils.deleteQuietly(tmpFile);
                                System.exit(0);
                            }
                        } else {
                            FileUtils.deleteQuietly(lockFile);
                            FileUtils.deleteQuietly(tmpFile);
                            LOGGER.info("deleted old lock file.");
                            break;
                        }
                    }
                }

            } while (!lockFile.exists());

            MinioManager minioManager = MinioManager.builder()
                    .minioConnection(MinioConnection.builder()
                            .endpoint(endpoint)
                            .accessKey(accessKey)
                            .accessSecret(secretKey)
                            .build())
                    .build();


            tmpFile = tmpFile.exists() ? tmpFile : new File(targetFileSavePath, tmpFile.getName());
            do {
                LOGGER.info("start to download file[{}], tmp file size:{}", fileFullName, tmpFile.length());
                try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile)) {
                    InputStream inputStream = minioManager.objectGetFromOffset(bucket, filePathInS3, tmpFile.length());
                    IOUtils.copy(inputStream, fileOutputStream);
                    fileOutputStream.flush();
                } catch (Exception e) {
                    LOGGER.error("download file failed, since {}", e.getMessage());
                }
                FileUtils.deleteQuietly(lockFile);
                tmpFile.renameTo(targetFile);
                LOGGER.info("renamed tmp file, download finished");
            } while (tmpFile.exists());

        }

        LOGGER.info("file[{}] already saved in dir[{}]", fileFullName, targetFileSavePath);
        System.exit(0);
    }

}