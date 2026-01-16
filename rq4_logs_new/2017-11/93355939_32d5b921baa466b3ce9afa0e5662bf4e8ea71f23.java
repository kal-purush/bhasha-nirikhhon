package ar.com.utn.proyecto.qremergencias.ws.service;

import ar.com.utn.proyecto.qremergencias.core.domain.UserFront;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;

@Aspect
@Component
@Slf4j
public class NotificationAspect {

    private static final String ANDROID_FCM_URL = "https://fcm.googleapis.com/fcm/send";

    private final PushNotificationBody body =
            new PushNotificationBody("Sus datos de emergencia han sido modificados", "QREmergencias");

    private final UserFrontService userFrontService;
    private final HttpHeaders headers = new HttpHeaders();

    @Value("${qremergencias.firebase.apiKey}")
    private String apiKey;

    @Autowired
    public NotificationAspect(final UserFrontService userFrontService) {
        this.userFrontService = userFrontService;
    }

    @PostConstruct
    public void init() {
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(HttpHeaders.AUTHORIZATION, "key=" + apiKey);
    }

    @AfterReturning("execution(* ar.com.utn.proyecto.qremergencias.ws"
            + ".controller.EmergencyDataController.updateEmergencyData(..))")
    public void sendEmergencyDataNotification(final JoinPoint joinPoint) {
        final String username = joinPoint.getArgs()[1].toString();
        final UserFront user = userFrontService.findByUsername(username);
        try {
            final PushNotification pushNotification = new PushNotification(user.getFirebaseToken(), body);
            final HttpEntity<PushNotification> request = new HttpEntity<>(pushNotification, headers);
            new RestTemplate().postForObject(ANDROID_FCM_URL, request, String.class);
        } catch (final Exception exception) {
            log.error("Error al enviar notificacion", exception);
        }
    }

    @Data
    private static class PushNotification {
        private final String to;
        private final PushNotificationBody notification;
    }

    @Data
    private static class PushNotificationBody {
        private final String text;
        private final String title;
    }
}