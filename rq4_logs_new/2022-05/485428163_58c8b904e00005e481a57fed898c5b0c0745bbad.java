package ebloodbanking.config;

import java.util.HashMap;
import java.util.Map;

public enum TestEnv {
    LOCAL("local"),
    DEVELOP("develop"),
    STAGING("staging");

    public final String label;

    TestEnv(String label) {
        this.label = label;
    }

    private static final Map<String, TestEnv> BY_LABEL = new HashMap<>();

    static {
        for (TestEnv appEnv : values()) {
            BY_LABEL.put(appEnv.label, appEnv);
        }
    }

    /** To get enum name from a label (choice is specified in choices.conf) */
    public static TestEnv parse(String label) {
        if (BY_LABEL.get(label) == null) {
            throw new IllegalStateException(String.format("%s is not a valid test env choice. Pick your test env from %s." +
                    "Check the value of 'TEST_ENV' property in choices.conf; Or in CI, if running from continuous integration.", label, BY_LABEL.keySet()));
        } else {
            return BY_LABEL.get(label);
        }
    }
}