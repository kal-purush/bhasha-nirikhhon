package com.reservibe.domain.output.management;

import com.reservibe.domain.entity.restaurant.Restaurant;
import com.reservibe.domain.generic.output.OutputInterface;
import com.reservibe.domain.generic.output.OutputStatus;

public class ManagementOutput implements OutputInterface {


    private final OutputStatus outputStatus;

    public ManagementOutput( OutputStatus outputStatus) {

        this.outputStatus = outputStatus;
    }

    @Override
    public Restaurant getBody() {

        return null;
    }

    @Override
    public OutputStatus getOutputStatus() {
        return outputStatus;
    }
}

