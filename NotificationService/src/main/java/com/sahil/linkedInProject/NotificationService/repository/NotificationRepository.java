package com.sahil.linkedInProject.NotificationService.repository;

import com.sahil.linkedInProject.NotificationService.entity.Notification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface NotificationRepository extends JpaRepository<Notification,Long> {
}
