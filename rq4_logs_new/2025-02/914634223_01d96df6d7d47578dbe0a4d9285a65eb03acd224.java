package tech.nocountry.c23e64.service;

import org.springframework.core.io.InputStreamSource;
import org.springframework.scheduling.annotation.Async;
import tech.nocountry.c23e64.model.RentalEntity;

public interface RentalEmailService {

    @Async
    void notifyClient(String to, RentalEntity rental, InputStreamSource qrCode);

}