package org.kylecorp.config;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.kylecorp.service.ParkingService;

public class ApplicationBinder extends AbstractBinder {
    @Override
    protected void configure() {
        bind(ParkingService.class).to(ParkingService.class);
    }
}