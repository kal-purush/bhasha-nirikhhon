package ie.atu.lab4_3;

import org.apache.catalina.User;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NotificationController {
    @PostMapping("/notification")
    public String notification(@RequestBody UserDetails userDetails){
        return "notification sent";
    }
}