package com.kaf.stream;

import java.util.List;

public class ExampleEnrichedRecord {
    public String name;
    public String planet;
    public String galaxy;
    public long flinked_at;

    public ExampleRecord.AseInternal ase_internal;

    // Getters and Setters

    public static class AseInternal {
        public boolean valid;
        public List<String> errors;
        public String schemaName;
        public int schemaVersion;
        public String timestamp;

        // Getters and Setters
    }
}