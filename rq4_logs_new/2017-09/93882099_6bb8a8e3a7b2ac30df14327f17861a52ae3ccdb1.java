package com.robothand.highqualitybot.util;

import net.dv8tion.jda.core.utils.SimpleLog;

/**
 * SimpleLogWrapper.java
 * Intercepts logging from JDA's SimpleLog and routes it through to SLF4J
 */
public class SimpleLogWrapper implements SimpleLog.LogListener {

    @Override
    public void onLog(SimpleLog log, SimpleLog.Level logLevel, Object message) {
        // TODO implement SimpleLogWrapper.onLog()
    }

    @Override
    public void onError(SimpleLog log, Throwable err) {
        // TODO implement SimpleLogWrapper.onError()
    }
}