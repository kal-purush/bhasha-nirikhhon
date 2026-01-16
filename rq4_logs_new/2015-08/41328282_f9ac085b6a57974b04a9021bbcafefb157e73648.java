package org.sagebionetworks.bridge.config;

import java.util.List;

public interface Config {

    /** Gets the user running the app. */
    String getUser();

    /** Gets the runtime environment. */
    Environment getEnvironment();

    /**
     * Gets the value of a config entry.
     * Do not include the "<environment>." prefix for the key.
     */
    String get(String key);

    /**
     * Gets the value of a config entry as an integer.
     * Do not include the "<environment>." prefix for the key.
     */
    int getInt(String key);

    /**
     * Gets the value of a config entry as a list.
     * Do not include the "<environment>." prefix for the key.
     */
    List<String> getList(String key);
}