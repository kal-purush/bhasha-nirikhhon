package spring.controller;

import spring.model.Payment;
import spring.service.PaymentService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@SuppressWarnings("unused")
@RestController
@RequestMapping("/api/payments")
public class PaymentRestController {
    private final PaymentService PaymentService;

    public PaymentRestController(PaymentService PaymentService, List<Payment> payment) {
        this.PaymentService = PaymentService;
    }

    @PostMapping("/post")
    public ResponseEntity<Payment> createPayment(@RequestBody Payment payment) {
        Payment createdPayment = PaymentService.createPayment(payment);
        return new ResponseEntity<>(createdPayment, HttpStatus.CREATED);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Payment> getPaymentById(@PathVariable long id) {
        Payment payment = PaymentService.getPaymentById(id);
        return ResponseEntity.ok(payment);
    }

    @GetMapping("/all")
    public ResponseEntity<List<Payment>> getAllPayments() {
        List<Payment> payments = PaymentService.getAllPayments();
        return ResponseEntity.ok(payments);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Payment> updatePayment(@PathVariable long id, @RequestBody Payment paymentDetails) {
        Payment updatedPayment = PaymentService.updatePayment(id, paymentDetails);
        return ResponseEntity.ok(updatedPayment);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deletePaymentById(@PathVariable long id) {
        PaymentService.deletePaymentById(id);
        return ResponseEntity.noContent().build();
    }
}