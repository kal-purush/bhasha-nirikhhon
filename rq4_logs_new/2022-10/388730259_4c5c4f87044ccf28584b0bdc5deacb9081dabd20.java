package org.egov.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class FiscalEventPostProcessorConfigTest {
    @Test
    void testInitialize() {
        FiscalEventPostProcessorConfig fiscalEventPostProcessorConfig = new FiscalEventPostProcessorConfig("UTC",
                "Fiscal Event Dereference Topic", "Fiscal Event Flattened Topic", "1234",
                "localhost", "Ifix Master Coa Context Path", "Ifix Master Coa Search Path", "localhost",
                "Ifix Master Government Context Path", "Ifix Master Government Search Path");
        fiscalEventPostProcessorConfig.initialize();
        assertEquals("Fiscal Event Dereference Topic", fiscalEventPostProcessorConfig.getFiscalEventDereferenceTopic());
        assertEquals("UTC", fiscalEventPostProcessorConfig.getTimeZone());
        assertEquals("Ifix Master Government Search Path",
                fiscalEventPostProcessorConfig.getIfixMasterGovernmentSearchPath());
        assertEquals("localhost", fiscalEventPostProcessorConfig.getIfixMasterGovernmentHost());
        assertEquals("Ifix Master Government Context Path",
                fiscalEventPostProcessorConfig.getIfixMasterGovernmentContextPath());
        assertEquals("Ifix Master Coa Search Path", fiscalEventPostProcessorConfig.getIfixMasterCoaSearchPath());
        assertEquals("localhost", fiscalEventPostProcessorConfig.getIfixMasterCoaHost());
        assertEquals("Ifix Master Coa Context Path", fiscalEventPostProcessorConfig.getIfixMasterCoaContextPath());
        assertEquals("Fiscal Event Flattened Topic", fiscalEventPostProcessorConfig.getFiscalEventFlattenedTopic());
        assertEquals("1234", fiscalEventPostProcessorConfig.getFiscalEventDruidTopic());
    }

    @Test
    void testJacksonConverter() {
        FiscalEventPostProcessorConfig fiscalEventPostProcessorConfig = new FiscalEventPostProcessorConfig("UTC",
                "Fiscal Event Dereference Topic", "Fiscal Event Flattened Topic", "1234",
                "localhost", "Ifix Master Coa Context Path", "Ifix Master Coa Search Path", "localhost",
                "Ifix Master Government Context Path", "Ifix Master Government Search Path");
        ObjectMapper objectMapper = new ObjectMapper();
        MappingJackson2HttpMessageConverter actualJacksonConverterResult = fiscalEventPostProcessorConfig
                .jacksonConverter(objectMapper);
        assertEquals(2, actualJacksonConverterResult.getSupportedMediaTypes().size());
        assertSame(objectMapper, actualJacksonConverterResult.getObjectMapper());
    }
}
