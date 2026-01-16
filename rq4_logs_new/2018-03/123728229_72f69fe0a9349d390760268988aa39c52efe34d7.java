package io.mart.util;

import cucumber.api.Scenario;
import jersey.repackaged.com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@Getter @Setter
public class ScenarioContext {
    private Set<String> tags;

    public void init(Scenario scenario) {
        tags = Sets.newHashSet(scenario.getSourceTagNames());
    }
}