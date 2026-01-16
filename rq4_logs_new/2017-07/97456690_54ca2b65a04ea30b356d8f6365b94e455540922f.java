package com.vinaysen.www;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class TraceProducerApplication extends Application<TraceProducerConfiguration> {

    public static void main(final String[] args) throws Exception {
        new TraceProducerApplication().run(args);
    }

    @Override
    public String getName() {
        return "TraceProducer";
    }

    @Override
    public void initialize(final Bootstrap<TraceProducerConfiguration> bootstrap) {
        // TODO: application initialization
    }

    @Override
    public void run(final TraceProducerConfiguration configuration,
                    final Environment environment) {
        // TODO: implement application
    }

}