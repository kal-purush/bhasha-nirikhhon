package com.bnau.cdb.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

public class TestUtil {

	/**
	 * Convert a DTO to the json format.
	 * 
	 * @param dto The object to convert.
	 * @return The converted json.
	 * @throws JsonProcessingException
	 */
	public static String dtoToJson(Object dto) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
		ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
		String requestJson = ow.writeValueAsString(dto);
		return requestJson;
	}
}