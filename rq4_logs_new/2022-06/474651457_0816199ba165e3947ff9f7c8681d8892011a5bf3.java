package com.example.projectmrsisa.listeners;

import com.example.projectmrsisa.service.ClientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.Calendar;

@Component
public class StartupApplicationListener implements ApplicationListener<ContextRefreshedEvent> {

    @Autowired
    private ClientService clientService;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        Calendar cal = Calendar.getInstance();
        int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
        if (dayOfMonth == 1) {
            clientService.updateClientsPenaltyStatus();
        }
    }
}