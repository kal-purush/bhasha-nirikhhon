package com.axonivy.connector.adobe.esign.connector.auth;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import ch.ivyteam.ivy.environment.Ivy;

public class JacksonUtils {
	private static final JaxRsClientJson JAXRS_CLIENT_JSON = new JaxRsClientJson();
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static JaxRsClientJson getJaxRsClientJsonProvider() {
		return JAXRS_CLIENT_JSON;
	}

	public static String writeObjectAsJson(Object entity) {
		try {
			return OBJECT_MAPPER.writeValueAsString(entity);
		} catch (JsonProcessingException e) {
			Ivy.log().warn(e.getMessage());
		}
		return null;
	}

	private static class JaxRsClientJson extends JacksonJsonProvider {
		@Override
		public ObjectMapper locateMapper(Class<?> type, MediaType mediaType) {
			ObjectMapper mapper = super.locateMapper(type, mediaType);
			// match our generated jax-rs client beans: that contain JSR310 data types
			mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
			// allow fields starting with an upper case character (e.g. in ODATA specs)!
			mapper.setConfig(mapper.getDeserializationConfig().with(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES));
			// not sending this optional value seems to be lass prone to errors for some
			// remote services.
			mapper.setSerializationInclusion(Include.NON_NULL);
			return mapper;
		}
	}

	public static <T> T convertJsonToObject(String json, Class<T> objectType) {
		if (StringUtils.isEmpty(json)) {
			return null;
		}
		try {
			return OBJECT_MAPPER.readValue(json, objectType);
		} catch (JsonProcessingException e) {
			Ivy.log().warn(e.getMessage());
		}
		return null;
	}
}