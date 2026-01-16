package com.maticmatic.sfgdi.controllers;

import com.maticmatic.sfgdi.services.GreetingService;

public class ConstructorInjectedController {

    private final GreetingService greetingService;
    public ConstructorInjectedController(GreetingService greetingService) {
        this.greetingService = greetingService;
    }

    public String GetGreeting() {
        return this.greetingService.sayGreeting();
    }
}