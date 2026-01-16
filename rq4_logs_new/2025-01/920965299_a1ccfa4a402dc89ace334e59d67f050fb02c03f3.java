/*
 *  Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.rule.validator;

/**
 * Constants used in the application
 */
public class Constants {
    public static final String OPENAPI_KEY = "openapi";
    public static final String SWAGGER_KEY = "swagger";
    public static final String OAS_3_1_VERSION = "3.1";
    public static final String OAS_3_0_VERSION = "3.0";
    public static final String ALIAS_PREFIX = "#";
    public static final String JSON_PATH_ROOT = "$";

    public static final String RULESET_FIELD_KEY = "@key";
    public static final String RULESET_ALPHABETICAL_KEYED_BY = "keyedBy";
    public static final String RULESET_CASING_TYPE = "type";
    public static final String RULESET_CASING_DISALLOW_DIGITS = "disallowDigits";
    public static final String RULESET_CASING_SEPARATOR = "separator";
    public static final String RULESET_CASING_SEPARATOR_CHAR = "char";
    public static final String RULESET_CASING_SEPARATOR_ALLOW_LEADING = "allowLeading";
    public static final String RULESET_ENUMERATION_VALUES = "values";
    public static final String RULESET_LENGTH_MIN = "min";
    public static final String RULESET_LENGTH_MAX = "max";
    public static final String RULESET_PATTERN_MATCH = "match";
    public static final String RULESET_PATTERN_NOT_MATCH = "notMatch";
    public static final String RULESET_SCHEMA_SCHEMA = "schema";
    public static final String RULESET_SCHEMA_DIALECT = "dialect";
    public static final String RULESET_SCHEMA_ALL_ERRORS = "allErrors";
    public static final String RULESET_XOR_PROPERTIES = "properties";
    public static final String RULESET_RULES = "rules";
    public static final String RULESET_ALIASES = "aliases";
    public static final String RULESET_FORMATS = "formats";
    public static final String DESCRIPTION = "description";
    public static final String RULESET_ALIAS_TARGETS = "targets";
    public static final String RULESET_GIVEN = "given";
    public static final String RULESET_FIELD = "field";
    public static final String RULESET_FUNCTION = "function";
    public static final String RULESET_FUNCTION_OPTIONS = "functionOptions";
    public static final String RULESET_EXTENDS = "extends";
    public static final String RULESET_MESSAGE = "message";
    public static final String RULESET_SEVERITY = "severity";
    public static final String RULESET_RECOMMENDED = "recommended";
    public static final String RULESET_RESOLVED = "resolved";
    public static final String RULESET_THEN = "then";

    public static final boolean RULESET_CASING_SEPARATOR_ALLOW_LEADING_DEFAULT = false;

    public static final String JSON_PATH_GROUPING_REGEX = "\\[(?:'([^']*)'|\"([^\"]*)\")\\]|([^.\\[\\]]+)";
    public static final String RULESET_ALPHABETICAL_REGEX = "^[a-zA-Z\\s.,!?;:'\"-]*$";
    public static final String RULESET_ALIAS_EXTRACTION_REGEX = "^#[a-zA-Z]+";
}