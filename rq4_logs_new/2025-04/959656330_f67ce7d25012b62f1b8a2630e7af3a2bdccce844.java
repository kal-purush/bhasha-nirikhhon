/*
 * Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.np.compilerplugin;

import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.tools.diagnostics.DiagnosticFactory;
import io.ballerina.tools.diagnostics.DiagnosticInfo;
import io.ballerina.tools.diagnostics.DiagnosticSeverity;
import io.ballerina.tools.diagnostics.Location;

import java.util.Collections;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Diagnostic code for the natural programming compiler plugin.
 *
 * @since 0.3.0
 */
class DiagnosticLog {
    private static final ResourceBundle MESSAGES = ResourceBundle.getBundle("diagnostics", Locale.getDefault());
    private static final String ERROR_PREFIX = "error";

    static void reportError(SyntaxNodeAnalysisContext ctx, CodeModifier.AnalysisData analysisData, Location location,
                            DiagnosticLog.DiagnosticCode diagnosticsCode, Object... args) {
//        String errorMessage = MessageFormat.format(
//                MESSAGES.getString(ERROR_PREFIX + "." + diagnosticsCode.getMessage()), args);
        String errorMessage = MESSAGES.getString(ERROR_PREFIX + "." + diagnosticsCode.getMessage());
        DiagnosticInfo diagnosticInfo =
                new DiagnosticInfo(diagnosticsCode.getCode(), errorMessage, DiagnosticSeverity.ERROR);
        analysisData.analysisTaskErrored = true;
        ctx.reportDiagnostic(
                DiagnosticFactory.createDiagnostic(diagnosticInfo, location, Collections.emptyList(), args));
    }

    enum DiagnosticCode {
        NON_JSON_EXPECTED_TYPE_NOT_YET_SUPPORTED("NP_ERROR_001", "non.json.expected.type.not.yet.supported"),
        UNEXPECTED_ARGUMENTS("NP_ERROR_002", "unexpected.arguments"),
        EXPECTED_A_SUBTYPE_OF_NP_MODEL("NP_ERROR_003", "expected.a.subtype.of.np.model");

        private final String code;
        private final String message;

        DiagnosticCode(String code, String message) {
            this.code = code;
            this.message = message;
        }

        public String getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
    }
}