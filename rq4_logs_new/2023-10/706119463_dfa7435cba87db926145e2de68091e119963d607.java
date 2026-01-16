package com.example.demo;

import lombok.Data;
import org.springframework.stereotype.Service;

@Data
@Service
public class AcknowledgeService {
    public String ackMessage(Details user)
    {
        String message = "Thank you" + user.getName() + " your request has been received";
        return message;
    }
}