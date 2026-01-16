package com.exasol.adapter.document.mapping;

/**
 * Behaviour for errors during schema mapping. This enum extends the {@link MappingErrorBehaviour} by the
 * CONVERT_OR_NULL and CONVERT_OR_ABORT option that is only applicable for certain types.
 */
public enum ConvertableMappingErrorBehaviour {

    /** Try to convert the value to string and return NULL if that is not possible. */
    CONVERT_OR_NULL,
    /** Try to convert the value to string and abort if that is not possible. */
    CONVERT_OR_ABORT,
    /** Abort the whole query */
    ABORT,
    /** Use NULL instead */
    NULL
}