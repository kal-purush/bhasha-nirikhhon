package cern.accsoft.steering.jmad.conf;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

/**
 * JMad service Java configuration. Use this class in Spring instead of the XMLs
 */
@Configuration
@ImportResource("cern/accsoft/steering/jmad/conf/app-ctx-jmad-service.xml")
public class JMadServiceConfiguration {
    /* Java configuration for hiding the xml */
}