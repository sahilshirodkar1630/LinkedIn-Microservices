package com.sahil.linkedInProject.NotificationService.service;

import com.sahil.linkedInProject.NotificationService.entity.Notification;
import com.sahil.linkedInProject.NotificationService.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationRepository notificationRepository;

    public void addNotification(Notification notification){
        log.info("Adding notification to db, content : {}",notification.getMessage());
        notification = notificationRepository.save(notification);

        //Send Mailer to send email

    }
}
