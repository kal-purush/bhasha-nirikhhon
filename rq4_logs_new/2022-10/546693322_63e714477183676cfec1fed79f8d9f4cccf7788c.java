package de.turing85.quarkus.extension.config.bug.deployment;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class QuarkusExtensionConfigBugProcessor {

    private static final String FEATURE = "quarkus-extension-config-bug";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }
}