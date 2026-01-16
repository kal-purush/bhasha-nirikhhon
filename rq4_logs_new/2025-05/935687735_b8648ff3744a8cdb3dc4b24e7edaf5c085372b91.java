package ru.itmo.icompiler.semantic.exception;

import java.util.Map;

public class RecordDuplicateFieldSemanticException extends SemanticException {
	private String property;
	
	public RecordDuplicateFieldSemanticException(String property, int errorLine, int errorOffset, int previousDeclaration) {
		super(
			String.format("property \"%s\" already declared in the record", property), 
			errorLine,
			errorOffset,
			Map.of("previously declared here", new int[] { previousDeclaration })
		);
		
		this.property = property;
	}
	
	public String getProperty() {
		return property;
	}
}