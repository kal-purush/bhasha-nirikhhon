package com.hoaxify.ws.file;

import com.hoaxify.ws.configuration.AppConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Created By Yasin Memic on May, 2020
 */
@Service
@EnableScheduling
public class FileCleanupService {

    @Autowired
    FileAttachmentRepository fileAttachmentRepository;

    @Autowired
    FileService fileService;

    private AppConfiguration appConfiguration;


    public FileCleanupService(AppConfiguration appConfiguration) {
        this.appConfiguration = appConfiguration;
    }

    @Scheduled(fixedRate = 10 * 1000)
    public void cleanupStorage() throws IOException {
        Date twentyFourHoursAgo = new Date(System.currentTimeMillis() - appConfiguration.getFileCleanupTime());
        List<FileAttachment> filesToBeDeleted = fileAttachmentRepository.findByDateBeforeAndHoaxIsNull(twentyFourHoursAgo);
        for (FileAttachment file : filesToBeDeleted) {
            fileService.deleteAttachmentFile(file.getName());
            fileAttachmentRepository.delete(file);
        }
    }
}