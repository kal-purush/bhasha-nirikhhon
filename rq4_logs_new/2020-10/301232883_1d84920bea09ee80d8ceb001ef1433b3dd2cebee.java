package tp.kafka.spam;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.arc.config.ConfigProperties;

@ConfigProperties(prefix = "spamprocessor")
public interface Configuration {
    
     @ConfigProperty(name="topic.in.input")
     public String inputTopic();
   
     @ConfigProperty(name="topic.out.filtered")
     public String outputTopic();

}