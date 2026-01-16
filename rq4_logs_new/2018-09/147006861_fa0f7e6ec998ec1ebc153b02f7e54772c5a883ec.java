package com.pw.sb.monitoring.monitors;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.actuate.endpoint.annotation.*;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Endpoint(id = "features", enableByDefault = true)
public class FeaturesConfigurer {

    private Map<String, Feature> features = new ConcurrentHashMap<>();

    @ReadOperation
    public Map<String, Feature> features() {
        return features;
    }

    @ReadOperation
    public Feature feature(@Selector String name) {

        features.put("a", new Feature(true, "add-user"));
        return features.get(name);
    }

    @WriteOperation
    public void configureFeature(@Selector String name, @Selector String featureName, @Selector Boolean enabled) {
        final Feature feature = new Feature(enabled, featureName);
        features.put(name, feature);
    }

    @DeleteOperation
    public void deleteFeature(@Selector String name) {
        features.remove(name);
    }

    public static class Feature {

        @Getter @Setter
        private Boolean enabled;

        @Getter @Setter
        private String name;

        public Feature(Boolean enabled, String name) {
            this.enabled = enabled;
            this.name = name;
        }
    }
}