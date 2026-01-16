package helper;

import net.tokensmith.pelican.config.PelicanAppConfig;

import java.util.Properties;

public class PubSubConfig extends PelicanAppConfig {

    @Override
    public Properties propertiesForSubscribe(String clientId, String consumerGroup) {
        Properties properties = super.propertiesForSubscribe(clientId, consumerGroup);
        properties.put("my.new.prop.key", "my.new.prop.value");
        return properties;
    }

    @Override
    public Properties propertiesForPublish(String clientId) {
        Properties properties = super.propertiesForPublish(clientId);
        properties.put("my.new.prop.key", "my.new.prop.value");
        return properties;
    }
}