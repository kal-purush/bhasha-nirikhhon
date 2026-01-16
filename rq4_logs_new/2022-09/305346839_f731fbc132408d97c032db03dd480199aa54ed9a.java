package com.exasol.errorreporting;

/**
 * The class models an error code parameter.
 */
public class ParameterDefinition {
    /** Replacement Parameter in case a parameter is missing its name */
    public static final ParameterDefinition UNDEFINED_PARAMETER = ParameterDefinition.builder("undefined parameter")
            .build();
    private final String name;
    private final Object value;
    private final String description;

    private ParameterDefinition(final Builder builder) {
        this.name = builder.name;
        this.value = builder.value;
        this.description = builder.description;
    }

    /**
     * Create a builder for an error code {@link ParameterDefinition}.
     *
     * @param name name of the parameter
     * @return parameter builder
     */
    public static ParameterDefinition.Builder builder(final String name) {
        return new Builder(name);
    }

    /**
     * Get the parameter name.
     *
     * @return name of the parameter
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the parameter value.
     *
     * @return value assigned to the parameter
     */
    public Object getValue() {
        return this.value;
    }

    /**
     * Get the parameter description.
     *
     * @return description of the parameter
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Check if the parameter has a name.
     *
     * @return {@code true} if the name is set.
     */
    public boolean hasName() {
        return this.name != null;
    }

    /**
     * Check if the value is set.
     *
     * @return {@code true} if the value is set.
     */
    public boolean hasValue() {
        return this.value != null;
    }

    /**
     * Check if the description is set.
     *
     * @return {@code true} if the description is set
     */
    public boolean hasDescription() {
        return this.description != null;
    }

    @Override
    public String toString() {
        if(hasValue()) {
            return this.value.toString();
        }
        else {
            return "<null>";
        }
    }

    /**
     * Builder for an error code {@link ParameterDefinition}.
     */
    public static class Builder {
        private final String name;
        private Object value;
        private String description;


        private Builder(final String name) {
            this.name = name;
        }

        /**
         * Set the parameter value.
         *
         * @param value value of the parameter
         * @return self for fluent programming
         */
        public Builder value(final Object value) {
            this.value = value;
            return this;
        }

        /**
         * Set the parameter description.
         *
         * @param description description of the parameter
         * @return self for fluent programming
         */
        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        /**
         * Build the {@link ParameterDefinition}.
         *
         * @return new {@link ParameterDefinition}
         */
        public ParameterDefinition build() {
            return new ParameterDefinition(this);
        }
    }
}