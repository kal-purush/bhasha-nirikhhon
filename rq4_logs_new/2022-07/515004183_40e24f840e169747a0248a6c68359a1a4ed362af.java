package io.codemc.maven.plugins.toolchain;

import com.google.common.collect.Maps;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.toolchain.MisconfiguredToolchainException;
import org.apache.maven.toolchain.ToolchainManagerPrivate;
import org.apache.maven.toolchain.ToolchainPrivate;

import java.util.Arrays;
import java.util.Properties;

@Mojo(name = "codemc-toolchain-selector", defaultPhase = LifecyclePhase.VALIDATE)
public class ToolchainSelectorMojo extends AbstractMojo {

    @Component
    private ToolchainManagerPrivate toolchainManagerPrivate;

    @Parameter(defaultValue = "${session}", readonly = true, required = true)
    private MavenSession session;

    @Parameter(property = "toolchainDefinition", required = true)
    String toolchainDefinition;

    @Override
    public void execute() throws MojoExecutionException {
        String[] splitted = toolchainDefinition.split(":");
        if (splitted.length != 2) {
            throw new MojoExecutionException("Invalid toolchain definition string!");
        }
        String type = splitted[0];
        String version = splitted[1];

        Properties properties = new Properties();
        properties.setProperty("version", version);
        try {
            ToolchainPrivate[] toolchains = toolchainManagerPrivate.getToolchainsForType(type, session);
            ToolchainPrivate selected = Arrays.stream(toolchains)
                    .filter(toolchain -> toolchain.matchesRequirements(Maps.fromProperties(properties)))
                    .findFirst().orElse(null);
            if (selected == null) {
                throw new MojoExecutionException("Toolchain not found!");
            }
            toolchainManagerPrivate.storeToolchainToBuildContext(selected, session);
        } catch (MisconfiguredToolchainException e) {
            throw new MojoExecutionException("An error occurred", e);
        }
    }
}