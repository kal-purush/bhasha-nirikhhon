package it.pagopa.pn.national.registries.utils;

import it.pagopa.pn.commons.configs.listeners.TaskIdApplicationListener;
import it.pagopa.pn.national.registries.PnNationalRegistriesApplication;
import org.springframework.boot.SpringApplication;

public class SpringApplicationUtils {

    public static SpringApplication buildSpringApplicationWithListener() {
        SpringApplication app = new SpringApplication(PnNationalRegistriesApplication.class);
        app.addListeners(new TaskIdApplicationListener());
        return app;
    }
}