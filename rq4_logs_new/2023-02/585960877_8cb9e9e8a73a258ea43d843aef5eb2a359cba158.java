package ru.otus.aop;

import ru.otus.aop.annotation.Log;

public class DefaultTestLogging implements TestLoggingInterface {
    @Log
    @Override
    public void calculation(int param1) {
        System.out.println("Call calculation1");
    }

    @Override
    public void calculation(int param1, int param2) {
        System.out.println("Call calculation2");
    }

    @Log
    @Override
    public void calculation(int param1, int param2, String param3) {
        System.out.println("Call calculation3");
    }
}