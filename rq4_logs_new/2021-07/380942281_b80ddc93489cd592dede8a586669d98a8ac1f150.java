package com.pk.atf;

import java.util.List;

import com.atlassian.oai.validator.report.ValidationReport;
import com.atlassian.oai.validator.schema.SchemaValidator;
import com.intuit.karate.core.Variable;

import io.swagger.v3.oas.models.media.Schema;

public class OASSchemaValidator {

	
	public void schemaValidatorMap(SchemaValidator schemaValidator, String objectToValidate, Schema schemaToValidate) {
		ValidationReport validationReport = schemaValidator.validate(objectToValidate, schemaToValidate, "prefix");
		if(validationReport.hasErrors()) {
			throw new RuntimeException(validationReport.getMessages().toString());
		}
	}
	
	public void schemaValidatorList(SchemaValidator schemaValidator, Variable actual, Schema schemaToValidate) {
		List list = actual.getValue();
		for(Object obj : list) {
			schemaValidatorMap(schemaValidator, obj.toString(), schemaToValidate);
		}
	}
	
}