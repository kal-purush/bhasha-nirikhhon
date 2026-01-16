package hu.bartl.bggprofileanalyzer.configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class XmlParsingConfiguration {
    
    @Bean
    public XmlMapper xmlMapper() {
        XmlMapper xmlMapper = new XmlMapper();
        SimpleModule module = new SimpleModule().addDeserializer(Object.class, XmlParsingConfiguration.Issue205FixedUntypedObjectDeserializer.getInstance());
        xmlMapper.registerModule(module);
        return xmlMapper;
    }
    
    @SuppressWarnings({"deprecation", "serial"})
    public static class Issue205FixedUntypedObjectDeserializer extends UntypedObjectDeserializer {
        
        private static final Issue205FixedUntypedObjectDeserializer INSTANCE = new Issue205FixedUntypedObjectDeserializer();
        
        private Issue205FixedUntypedObjectDeserializer() {}
        
        public static Issue205FixedUntypedObjectDeserializer getInstance() {
            return INSTANCE;
        }
        
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        protected Object mapObject(JsonParser parser, DeserializationContext context) throws IOException {
            
            // Read the first key.
            String firstKey;
            JsonToken token = parser.getCurrentToken();
            if (token == JsonToken.START_OBJECT) {
                firstKey = parser.nextFieldName();
            } else if (token == JsonToken.FIELD_NAME) {
                firstKey = parser.getCurrentName();
            } else {
                if (token != JsonToken.END_OBJECT) {
                    throw context.mappingException(handledType(), parser.getCurrentToken());
                }
                return Collections.emptyMap();
            }
            
            // Populate entries.
            Map<String, Object> valueByKey = new LinkedHashMap<>();
            String nextKey = firstKey;
            do {
                
                // Read the next value.
                parser.nextToken();
                Object nextValue = deserialize(parser, context);
                
                // Key conflict? Combine existing and current entries into a list.
                if (valueByKey.containsKey(nextKey)) {
                    Object existingValue = valueByKey.get(nextKey);
                    if (existingValue instanceof List) {
                        List<Object> values = (List<Object>) existingValue;
                        values.add(nextValue);
                    } else {
                        List<Object> values = new ArrayList<>();
                        values.add(existingValue);
                        values.add(nextValue);
                        valueByKey.put(nextKey, values);
                    }
                } else {
                    valueByKey.put(nextKey, nextValue);
                }
                
            } while ((nextKey = parser.nextFieldName()) != null);
            
            return valueByKey;
            
        }
    }
}