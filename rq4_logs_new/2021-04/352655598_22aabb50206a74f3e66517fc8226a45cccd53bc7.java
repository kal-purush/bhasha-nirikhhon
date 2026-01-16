package com.kodilla.ecommerce.controller;

import com.kodilla.ecommerce.domain.Order;
import com.kodilla.ecommerce.domain.User;
import com.kodilla.ecommerce.domain.enums.StatusOrder;
import com.kodilla.ecommerce.service.EmailService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.RestController;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

@RestController
@AllArgsConstructor
public class EmailController {

    private final EmailService emailService;
    private final TemplateEngine templateEngine;

    public void sendEmail(final User user, final Order order) {
        Context context = new Context();
        context.setVariable("header", "Witaj, " + user.getName());
        context.setVariable("title", "Twoje zamówienie o numerze " + order.getNumber() + " zmieniło status:");
        if (order.getStatus() == StatusOrder.ACCEPTED) {
            context.setVariable("description", "ZAAKCEPTOWANE!");
        } if (order.getStatus() == StatusOrder.IN_PROGRESS) {
            context.setVariable("description", "PRZYJĘTE DO REALIZACJI!");
        } if (order.getStatus() == StatusOrder.SENT) {
            context.setVariable("description", "WYSŁANE!");
        } if (order.getStatus() == StatusOrder.CANCELED) {
            context.setVariable("description", "ANULOWANE!");
        }
        String body = templateEngine.process("template", context);
        emailService.sendEmail(user.getEmail(), "Zmiana statusu zamówienia!", body);
    }
}