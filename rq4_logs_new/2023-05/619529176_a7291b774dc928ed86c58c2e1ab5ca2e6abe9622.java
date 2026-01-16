package kim.almroth.javazeebee;


import kim.almroth.javazeebee.message.dto.PublishRequest;
import kim.almroth.javazeebee.message.dto.StartSingleRequest;
import kim.almroth.javazeebee.start.dto.StartRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

@RestController()
@RequestMapping("api")
public class CamundaController {

    @Autowired
    CamundaService service;
    @PostMapping("/message")
    public ResponseEntity<?> publish(@RequestBody PublishRequest request, @RequestHeader(HttpHeaders.AUTHORIZATION) String token) throws Exception {
        System.out.println("Got message request for " + request.getBusiness() + " " + request.getForYearMonth());
        if (!service.isTokenValid(token)) {
            System.out.println("Invalid JWT");
            return ResponseEntity.status(401).body("JWT is not valid");
        }
        System.out.println("Token is valid");

        if (service.updateSeat(request, token)){
            var key = service.publish(request);
        } else {
            ResponseEntity.badRequest().body("Failed to update");
        }

        return ResponseEntity.ok().build();
    }

    @PostMapping("/camunda/start/single")
    public ResponseEntity<?> startSingleReport(@RequestBody StartSingleRequest request, @RequestHeader(HttpHeaders.AUTHORIZATION) String token) throws Exception {
        System.out.println("Starting single seat...");
        if (!service.isTokenValid(token)) {
            System.out.println("Invalid JWT");
            return ResponseEntity.status(401).body("JWT is not valid");
        }
        System.out.println("Token is valid");

        service.startSingleReport(request);

        return ResponseEntity.noContent().build();
    }

    @PostMapping("/camunda/start")
    public ResponseEntity<?> startMonthlyReports(@RequestBody StartRequest request, @RequestHeader(HttpHeaders.AUTHORIZATION) String token) throws ExecutionException, InterruptedException {
        System.out.println("Starting camunda process instance");
        service.startMonthlyReports(request);
        return ResponseEntity.noContent().build();
    }
}