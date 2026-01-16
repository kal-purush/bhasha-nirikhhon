package com.andrey.currencyconverter.app;

import com.andrey.currencyconverter.controllers.impl.ConsoleControllerImpl;
import com.andrey.currencyconverter.repo.impl.JsonFileRepositoryImpl;
import com.andrey.currencyconverter.view.Impl.ConsoleInterfaceImplementation;

public class App
{
    public static void main( String[] args ) {
        ApplicationFlow applicationFlow = new ConsoleApplicationFlow();
        applicationFlow.startFlow(
                new ConsoleInterfaceImplementation(),
                new ConsoleControllerImpl(
                        new JsonFileRepositoryImpl()));
    }
}