package org.wso2.rule.validator.validator;

import org.wso2.rule.validator.DiagnosticSeverity;

/**
 * Class to represent the result of a document validation that is sent to the user
 */
public class DocumentValidationResult {
    public final String path;
    public final String message;
    public final String ruleName;
    public final String severity;

    public DocumentValidationResult(String path, String message, String ruleName, DiagnosticSeverity severity) {
        this.path = path;
        this.message = message;
        this.ruleName = ruleName;
        this.severity = DiagnosticSeverity.getSeverityString(severity);
    }
}