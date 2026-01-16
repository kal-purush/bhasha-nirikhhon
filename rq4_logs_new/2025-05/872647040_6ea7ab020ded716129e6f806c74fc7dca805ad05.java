package com.jgp.infrastructure.bulkimport.listener;

import com.jgp.authentication.domain.AppUser;
import com.jgp.authentication.service.UserService;
import com.jgp.infrastructure.bulkimport.data.GlobalEntityType;
import com.jgp.infrastructure.bulkimport.event.DataApprovedEvent;
import com.jgp.infrastructure.bulkimport.event.DataUploadedEvent;
import com.jgp.notification.service.EmailService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class DataUploadedEventListener {

    private final UserService userService;
    private final EmailService emailService;

    @EventListener
    @Async
    public void handleDataUploadedEvent(DataUploadedEvent dataUploadedEvent){


        final var entityType = dataUploadedEvent.entityType();
        List<AppUser> dataReviewUsers = new ArrayList<>();
        if (GlobalEntityType.LOAN_IMPORT_TEMPLATE.equals(entityType)){
            dataReviewUsers = this.userService.findUsersByPartnerId(dataUploadedEvent.partnerId()).stream()
                    .filter(user -> user.hasAnyPermission("LOAN_APPROVE"))
                    .toList();
        } else if (GlobalEntityType.TA_IMPORT_TEMPLATE.equals(entityType)) {
            dataReviewUsers = this.userService.findUsersByPartnerId(dataUploadedEvent.partnerId()).stream()
                    .filter(user -> user.hasAnyPermission("BMO_PARTICIPANTS_DATA_APPROVE"))
                    .toList();
        }
        if(dataReviewUsers.isEmpty()){
            return;
        }

        for (var appUser: dataReviewUsers){
            this.emailService.sendEmailNotificationForDataReview(appUser.getUsername(), appUser.getUserFullName(), entityType.getDataType());
        }
    }
}