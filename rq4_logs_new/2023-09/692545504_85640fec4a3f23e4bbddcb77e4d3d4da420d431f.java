package org.benbroadaway.unifi.cli.mixins;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command
public class Log {
    private static final Logger LOGGER = LoggerFactory.getLogger(Log.class);

    @Option(names = {"-v", "--verbose"},
            description = "Increase verbosity. Specify multiple times to increase (-vvv).")
    boolean[] verbosity = new boolean[0];

    public void error(String pattern, Object... params) {
        LOGGER.error(pattern, params);
    }

    public void warn(String pattern, Object... params) {
        LOGGER.warn(pattern, params);
    }

    public void info(String pattern, Object... params) {
        LOGGER.info(pattern, params);
    }

    public void debug(String pattern, Object... params) {
        if (verbosity.length > 0) {
            LOGGER.debug(pattern, params);
        }
    }

    public void trace(String pattern, Object... params) {
        if (verbosity.length > 1) {
            LOGGER.trace(pattern, params);
        }
    }
}