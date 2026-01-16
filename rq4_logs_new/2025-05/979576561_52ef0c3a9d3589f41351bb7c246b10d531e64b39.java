package software.sebastian.oposiciones;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.sebastian.oposiciones.service.NotificationService;

@Component
public class NotificationCleanupTask {

    @Autowired
    private NotificationService notificationService;

    // Cada día a la 1:00 AM
    @Scheduled(cron = "0 0 1 * * ?")
    public void cleanOldNotifications() {
        notificationService.deleteOldNotifications();
    }
}