package org.example.atharvolunteeringplatform.Controller;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.example.atharvolunteeringplatform.Model.PaymentRequest;
import org.example.atharvolunteeringplatform.Service.PaymentService;
import org.example.atharvolunteeringplatform.Service.StudentService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;
    private final StudentService studentService;

    @Value("${moyasar.api.key}")
    private String apiKey;

    @PostMapping("/card")
    public ResponseEntity processPayment(@RequestBody PaymentRequest paymentRequest) {
        return ResponseEntity.status(HttpStatus.OK).body(paymentService.processPayment(paymentRequest));
    }

    @RequestMapping(value = "/callback", method = {RequestMethod.GET, RequestMethod.POST})
    public ResponseEntity<String> handleCallback(HttpServletRequest request) {
        String paymentId = request.getParameter("id");
        String studentIdStr = request.getParameter("studentId");

        if (paymentId == null || studentIdStr == null) {
            return ResponseEntity.badRequest().body("Missing paymentId or studentId");
        }

        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setBasicAuth(apiKey, "");
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        String url = "https://api.moyasar.com/v1/payments/" + paymentId;
        ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.GET, entity, Map.class);

        String status = (String) response.getBody().get("status");

        if ("paid".equalsIgnoreCase(status)) {
            Integer studentId = Integer.parseInt(studentIdStr);
            studentService.requestCertificate(studentId);
            return ResponseEntity.ok("Payment successful and certificate sent.");
        }

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Payment failed.");
    }

}