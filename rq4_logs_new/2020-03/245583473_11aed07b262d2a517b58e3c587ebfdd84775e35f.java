package systems.reformcloud.reformcloud2.prefixes.config;

import systems.reformcloud.reformcloud2.executor.api.common.configuration.JsonConfiguration;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Collections;

public final class ConfigParser {

    private ConfigParser() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    public static JsonConfiguration getConfig(@Nonnull File dataFolder) {
        File config = new File(dataFolder, "config.json");
        if (!config.exists()) {
            new JsonConfiguration()
                    .add("chatFormat", "%s §c>§f %s")
                    .add("prefixes", Collections.singletonMap("default", "§7Player §8> "))
                    .write(config);
        }

        return JsonConfiguration.read(config);
    }
}