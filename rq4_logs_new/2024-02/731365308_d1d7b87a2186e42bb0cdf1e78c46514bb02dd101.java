public class LogLine {
    private String logLine ;
    public LogLine(String logLine) {
        this.logLine = logLine ;
    }

    public LogLevel getLogLevel() {
        if (logLine.startsWith("[TRC]")) {
            return LogLevel.TRACE;
        } else if (logLine.startsWith("[DBG]")) {
            return LogLevel.DEBUG;
        } else if (logLine.startsWith("[INF]")) {
            return LogLevel.INFO;
        } else if (logLine.startsWith("[WRN]")) {
            return LogLevel.WARNING;
        } else if (logLine.startsWith("[ERR]")) {
            return LogLevel.ERROR;
        } else if (logLine.startsWith("[FTL]")) {
            return LogLevel.FATAL;
        } else {
            return LogLevel.UNKNOWN;
        }
    }

    public String getOutputForShortLog() {
        LogLevel logLevel = getLogLevel();
        String[] parts = logLine.split(":");
        String message = parts[1].trim();

        return logLevel.getEncodedLevel() + ":" + message;
    }
}