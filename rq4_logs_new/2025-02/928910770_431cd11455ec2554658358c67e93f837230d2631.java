package com.example.lamp;
import org.springframework.stereotype.Component;

@Component
class Fan {
    void turnOn() {
        System.out.println("🌀 Вентилятор включен!");
    }
}