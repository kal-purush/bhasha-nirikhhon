package europeana.sparql.updater.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

/**
 * Utility class to log processing progress after roughly ever x seconds.
 * @author Patrick Ehlert
 */
public class ProgressLogger {

    private static final Logger LOG = LogManager.getLogger(ProgressLogger.class);

    private static final int MS_PER_SEC = 1_000;
    private static final int SEC_PER_MIN = 60;
    private static final int MIN_PER_HOUR = 24;

    private final long startTime;
    private final long totalItems;
    private long itemsDone;
    private final int logAfterSeconds;
    private long lastLogTime;

    /**
     * Create a new progressLogger. This also sets the operation start time
     * @param totalItems total number of items that are expected to be retrieved
     * @param logAfterSeconds logs are generated only after every <logAfterSeconds> seconds, even if the method is called more often
     */
    public ProgressLogger(long totalItems, int logAfterSeconds) {
        this.startTime = System.currentTimeMillis();
        this.lastLogTime = startTime;
        this.totalItems = totalItems;
        this.itemsDone = 0;
        this.logAfterSeconds = logAfterSeconds;
    }


    /**
     * Log that another set was processed, plus add an estimate of the remaining processing time (output og only if at
     * least x seconds have past since the last log, as specified by logAfterSeconds variable)
     */
    public synchronized void logItemAdded() {
        long now = System.currentTimeMillis();
        this.itemsDone++;
        Duration d = Duration.ofMillis(now - lastLogTime);

        if (logAfterSeconds > 0 && d.getSeconds() >= logAfterSeconds) {
            if (totalItems > 0) {
                double itemsPerMS = itemsDone * 1D / (now - startTime);
                if (LOG.isInfoEnabled() && itemsPerMS * MS_PER_SEC * SEC_PER_MIN > 3) {
                    LOG.info("Retrieved {} sets of {} ({} sets/min). Expected time remaining is {}", itemsDone, totalItems,
                            Math.round(itemsPerMS * MS_PER_SEC * SEC_PER_MIN),
                            getDurationText(Math.round((totalItems - itemsDone) / itemsPerMS)));
                } else if (LOG.isInfoEnabled()) {
                    LOG.info("Retrieved {} sets of {} ({} sets/hour). Expected time remaining is {}", itemsDone, totalItems,
                            Math.round(itemsPerMS * MS_PER_SEC * SEC_PER_MIN * MIN_PER_HOUR),
                            getDurationText(Math.round((totalItems - itemsDone) / itemsPerMS)));
                }
            } else {
                LOG.info("Retrieved {} items", itemsDone);
            }
            lastLogTime = now;
        }
    }

    /**
     * Log a duration in easy readable text
     * @param durationInMs duration to output
     * @return string containing duration in easy readable format
     */
    public static String getDurationText(long durationInMs) {
        String result;
        Duration d = Duration.ofMillis(durationInMs);
        if (d.toDaysPart() >= 1) {
            result = String.format("%d days, %d hours and %d minutes", d.toDaysPart(), d.toHoursPart(), d.toMinutesPart());
        } else if (d.toHoursPart() >= 1) {
            result = String.format("%d hours and %d minutes", d.toHoursPart(), d.toMinutesPart());
        } else if (d.toMinutesPart() >= 1) {
            result = String.format("%d minutes and %d seconds", d.toMinutesPart(), d.toSecondsPart());
        } else if (d.getSeconds() >= 10) {
            result = String.format("%d seconds", d.toSeconds());
        } else if (d.getSeconds() >= 2){
            result = String.format("%d seconds and %d milliseconds", d.toSecondsPart(), d.toMillisPart());
        } else {
            result = String.format("%d milliseconds", d.toMillis());
        }
        return result;
    }
}