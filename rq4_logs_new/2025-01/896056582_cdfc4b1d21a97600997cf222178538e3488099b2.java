package PlanQ.PlanQ.notification;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/v1/notification")
@RequiredArgsConstructor
public class NotificationController {
    private final NotificationService notificationService;

    @GetMapping(value = "/connect", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<SseEmitter> subscribe(@RequestHeader(value = "Last-Event_Id", required = false, defaultValue = "") String lastEventId){
        SseEmitter emitter = notificationService.subscribe(lastEventId);
        return ResponseEntity.ok(emitter);
    }

    @Operation(summary = "알림 보기", description = "알림 보기")
    @GetMapping("/view/{notificationId}")
    public ResponseEntity<ResponseNotificationDto> viewNotification(@PathVariable Long notificationId){
        ResponseNotificationDto response = notificationService.findByNotification(notificationId);
        return ResponseEntity.ok(response);
    }

}