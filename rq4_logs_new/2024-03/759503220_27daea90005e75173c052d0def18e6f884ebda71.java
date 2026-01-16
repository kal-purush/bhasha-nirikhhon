package edu.java.scrapper;

import edu.java.configuration.ProviderConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.Environment;
import static org.assertj.core.api.Assertions.assertThat;

class ProviderConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner();

    @Test
    void whenPropertiesAreProvided_thenTheyAreSet() {
        contextRunner.withPropertyValues(
                         "provider.github=github-url-test",
                         "provider.stackoverflow=stackoverflow-url-test")
                     .run(context -> {
                         Environment environment = context.getEnvironment();
                         ProviderConfig config = Binder.get(environment)
                                                       .bind("provider", ProviderConfig.class)
                                                       .get();

                         assertThat(config.getGithub()).isEqualTo("github-url-test");
                         assertThat(config.getStackoverflow()).isEqualTo("stackoverflow-url-test");
                     });
    }

    @Test
    void whenPropertiesAreNotProvided_thenDefaultValuesAreSet() {
        String defaultGitHub = "https://api.github.com";
        String defaultStackOverFlow = "https://api.stackexchange.com/2.3";
        ProviderConfig providerConfig = new ProviderConfig(
            defaultGitHub,
            defaultStackOverFlow
        );
        contextRunner.run(context -> {
            Environment environment = context.getEnvironment();
            ProviderConfig config = Binder.get(environment)
                                          .bind("provider", ProviderConfig.class)
                                          .orElse(providerConfig);

            assertThat(config.getGithub()).isEqualTo(defaultGitHub);
            assertThat(config.getStackoverflow()).isEqualTo(defaultStackOverFlow);
        });
    }
}