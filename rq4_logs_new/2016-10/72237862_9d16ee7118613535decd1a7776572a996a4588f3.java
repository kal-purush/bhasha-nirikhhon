package com.nulp.rock.common;

import org.testng.Assert;

import java.text.MessageFormat;

import static org.testng.Assert.assertTrue;


public class Asserter {

    private static boolean isAsserter = false;

    private final String messagePattern = "Verification was not successful, expected value: \"{0}\", but obtained \"{1}\"";

    public static String error = "";

    public Asserter() {
    }

    private enum ErrorLevel {
        FAIL, ERROR, PASS, INFO, WARNING, ENV, DEBUG;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Asserter))
            return false;

        Asserter asserter = (Asserter) o;

        return true;
    }

    public void assertEquals(Object actualObject, Object expectedObject, String failMessage, String passMessage) {
        try {
            isAsserter = true;
            assertEquals(actualObject, expectedObject, failMessage, passMessage, ErrorLevel.FAIL);
        } catch (Error e) {
            throw e;
        } finally {
            isAsserter = false;
        }

    }

    public void assertEquals(Object actualObject, Object expectedObject, String failMessage, String passMessage, ErrorLevel errorLevel) {
        if (actualObject == expectedObject) {
            Logger.logDebug(passMessage);
            return;
        }

        if (actualObject.equals(expectedObject)) {
            Logger.logDebug(passMessage);
        } else {
            log(MessageFormat.format(messagePattern, actualObject.toString(), expectedObject.toString()), errorLevel);
            Assert.assertEquals(actualObject, expectedObject, failMessage);
        }
    }

    public void assertFail(boolean condition, String failMessage, String passMessage) {
        assertCondition(condition, failMessage, passMessage, ErrorLevel.FAIL);
    }

    public void assertFalse(boolean condition, String failMessage, String passMessage) {
        assertCondition(!condition, failMessage, passMessage, ErrorLevel.PASS);
    }

    public void assertError(boolean condition, String failMessage, String passMessage) {
        assertCondition(condition, failMessage, passMessage, ErrorLevel.ERROR);
    }

    public void assertPass(boolean condition, String failMessage, String passMessage) {
        assertCondition(condition, failMessage, passMessage, ErrorLevel.PASS);
    }

    public void assertInfo(boolean condition, String failMessage, String passMessage) {
        assertCondition(condition, failMessage, passMessage, ErrorLevel.INFO);
    }

    public void assertWarning(boolean condition, String failMessage, String passMessage) {
        assertCondition(condition, failMessage, passMessage, ErrorLevel.WARNING);
    }

    public void assertEnv(boolean condition, String failMessage, String passMessage) {
        assertCondition(condition, failMessage, passMessage, ErrorLevel.ENV);
    }

    public void assertDebug(boolean condition, String failMessage, String passMessage) {
        assertCondition(condition, failMessage, passMessage, ErrorLevel.DEBUG);
    }

    private void assertCondition(boolean condition, String failMessage, String passMessage, ErrorLevel level) {
        try {
            isAsserter = true;

            if (!condition) {
                Logger.setState(false);
                log(failMessage, ErrorLevel.FAIL);
                assertTrue(condition, failMessage);
            } else {
                Logger.setState(true);
                Logger.logDebug(passMessage);
            }
        } catch (Error e) {
            throw e;
        } finally {
            isAsserter = false;
        }
    }

    private void log(String message, ErrorLevel level) {
        switch (level) {
            case FAIL:
                Logger.logFail(message);
                break;
            case ERROR:
                Logger.logError(message);
                break;
            case PASS:
                Logger.logPass(message);
                break;
            case INFO:
                Logger.logInfo(message);
                break;
            case WARNING:
                Logger.logWarning(message);
                break;
            case ENV:
                Logger.logEnvironment(message);
                break;
            case DEBUG:
                Logger.logDebug(message);
                break;
        }
    }
}