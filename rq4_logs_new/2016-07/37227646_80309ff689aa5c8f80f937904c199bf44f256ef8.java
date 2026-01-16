package com.example.andras.myapplication.dagger;

import javax.inject.Inject;

/**
 * Created by Andras_Nemeth on 2016. 07. 28..
 */
public class Dependency1FakeImpl implements Dependency1 {


    @Inject
    public Dependency1FakeImpl() {
    }

    @Override
    public String getStart() {
        return "I'm fake!";
    }
}