package ar.gov.santafe.meduc.serviceLocator;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class CustomJsonProvider extends org.codehaus.jackson.jaxrs.JacksonJsonProvider {

    public CustomJsonProvider() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationConfig.Feature.WRAP_ROOT_VALUE,false);
        mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT,true);
        setMapper(mapper);
    }

}