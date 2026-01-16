package com.hiapoe.lumiere.server.lumiere_server.components;

import com.hiapoe.lumiere.server.lumiere_server.properties.FileStorageServiceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

@Component
public class ScheduledTasksComponent {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledTasksComponent.class);
    private final Path finalDestinationDirectoryLocation;
    private final Path uploadDirectory;

    @Autowired
    public ScheduledTasksComponent(FileStorageServiceProperties fileStorageServiceProperties) {
        this.finalDestinationDirectoryLocation = Paths.get(fileStorageServiceProperties.getFinalDestinationDirectory()).toAbsolutePath().normalize();
        this.uploadDirectory = Paths.get(fileStorageServiceProperties.getUploadDirectory()).toAbsolutePath().normalize();
    }

    @Scheduled(cron = "${file-storage-service.final-destination-cron}")
    public void sendStoredFilesToFinalDestination() {
        if (Objects.isNull(this.finalDestinationDirectoryLocation) || this.finalDestinationDirectoryLocation.equals(Paths.get(""))) {
            ScheduledTasksComponent.logger.info("sendStoredFilesToFinalDestination feature disabled: no action necessary");
            return;
        }
        ScheduledTasksComponent.logger.info(String.format("Sending uploaded files to final destination at: %s", this.finalDestinationDirectoryLocation));
        try {
            Files.list(this.uploadDirectory).map(Path::getFileName)
                .forEach(directory -> {
                    Path finalDirectoryPath = this.finalDestinationDirectoryLocation.resolve(directory);
                    Path initialDirectoryPath = this.uploadDirectory.resolve(directory);
                    try {
                        if (Files.exists(finalDirectoryPath)) {
                            Files.list(initialDirectoryPath).forEach(fileToMove -> {
                                try {
                                    Files.move(fileToMove, finalDirectoryPath.resolve(fileToMove.getFileName()));
                                } catch (IOException e) {
                                    ScheduledTasksComponent.logger.error(String.format("unable to move file %s to %s", fileToMove.getFileName(), this.finalDestinationDirectoryLocation), e);
                                }
                            });
                            Files.delete(initialDirectoryPath);
                        }
                        else {
                            Files.move(initialDirectoryPath, finalDirectoryPath, StandardCopyOption.REPLACE_EXISTING);
                        }
                        ScheduledTasksComponent.logger.info(String.format("%s directory moved to final destination: %s", directory, finalDirectoryPath));
                    }
                    catch (IOException e) {
                        ScheduledTasksComponent.logger.warn(String.format("unable to move the directory %s to %s", directory, this.finalDestinationDirectoryLocation), e);
                    }
            });
        }
        catch (IOException e) {
            ScheduledTasksComponent.logger.error(String.format("unable to move any directory to %s", this.finalDestinationDirectoryLocation), e);
        }
    }
}