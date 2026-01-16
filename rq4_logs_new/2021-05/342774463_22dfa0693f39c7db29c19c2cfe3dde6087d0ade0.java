package util.log;

import android.os.ParcelFileDescriptor;
import android.util.Log;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Prints formatted messages to the Android LogCat.
 *
 * NOTE: We use {@link String#concat} to concatenate pairs of strings there cause it was
 * proven that it requires less CPU time than other concatenation utilities
 * (plus sign, explicit StreamBuilder and StreamBuffer objects).
 */
public final class JavaLogger {

    @SuppressWarnings("CharsetObjectCanBeUsed")
    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    /** Use small buffer size to make sure that text got written almost line-by-line.  */
    private static final int SZ_WRITE_BUFFER = 128;

    /** Length of log timestamp with the space at the end. */
    private static final int SZ_MSG_TIMESTAMP = 19;

    /** Upper-bound estimate of the log message prefix length. */
    private static final int SZ_THREAD_INFO = 19;

    /** Minimum length of place reserved by LogCat for process tag. */
    private static final int SZ_LOGCAT_TAG = 8;

    private static final int PROCESS_ID = android.os.Process.myPid();

    private static final ThreadLocal<ThreadLocals> THREAD_LOCALS = new ThreadLocal<ThreadLocals>() {

        @Override
        protected ThreadLocals initialValue() {
            return new ThreadLocals(new GregorianCalendar(Locale.US), android.os.Process.myTid());
        }
    };

    /**
     * Stores the tags of the process which performs logging.
     *
     * This field is expected to stay non-null and unmodified after {@link #init} method invocation.
     */
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean();

    /**
     * Stores the tags of the process which performs logging.
     *
     * This field is expected to stay not modified after {@link #init} method invocation.
     */
    @NonNull
    private static volatile String sProcessTag = "";

    /**
     * Writes logs to the file specified by configuration.
     *
     * This field is expected to stay not modified after {@link #init} method invocation.
     */
    @Nullable
    private static volatile PrintWriter sWriter;

    @NonNull
    private final String mBracedClassTag;

    /* default */ static void init(@NonNull LogConfiguration configuration) {
        if (INITIALIZED.getAndSet(true)) {
            throw new IllegalStateException("Java logger is already initialized.  "
                    + "This method should be invoked once per process.");
        }
        sProcessTag = configuration.getProcessTag();
        final ParcelFileDescriptor logFileDescriptor = configuration.getLogFileDescriptor();
        if (logFileDescriptor != null) {
            final OutputStream os = new FileOutputStream(logFileDescriptor.getFileDescriptor());
            sWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, CHARSET_UTF8), SZ_WRITE_BUFFER));
        }
    }

    /* default */ JavaLogger(@NonNull String bracedClassTag) {
        mBracedClassTag = bracedClassTag;
    }

    public void v(@NonNull String msg) {
        final String logcatMsg = mBracedClassTag.concat(msg);
        final String processTag = sProcessTag;
        final PrintWriter writer = sWriter;
        if (writer == null) {
            Log.v(processTag, logcatMsg);
        } else {
            final ThreadLocals threadLocals = THREAD_LOCALS.get();
            final String threadInfo = formatThreadInfo(threadLocals.threadId, 'V', processTag);
            // write under lock in order to keep messages synchronized across sinks
            synchronized (JavaLogger.class) {
                Log.v(processTag, logcatMsg);
                // get timestamp inside lock so as to make it as precise as possible
                final String timestamp = formatTimestamp(threadLocals.calendar);
                writer.append(timestamp).append(threadInfo).println(logcatMsg);
            }
        }
    }

    public void d(@NonNull String msg) {
        final String logcatMsg = mBracedClassTag.concat(msg);
        final String processTag = sProcessTag;
        final PrintWriter writer = sWriter;
        if (writer == null) {
            Log.d(processTag, logcatMsg);
        } else {
            final ThreadLocals threadLocals = THREAD_LOCALS.get();
            final String threadInfo = formatThreadInfo(threadLocals.threadId, 'D', processTag);
            // write under lock in order to keep messages synchronized across sinks
            synchronized (JavaLogger.class) {
                Log.d(processTag, logcatMsg);
                // get timestamp inside lock so as to make it as precise as possible
                final String timestamp = formatTimestamp(threadLocals.calendar);
                writer.append(timestamp).append(threadInfo).println(logcatMsg);
            }
        }
    }

    public void i(@NonNull String msg) {
        final String logcatMsg = mBracedClassTag.concat(msg);
        final String processTag = sProcessTag;
        final PrintWriter writer = sWriter;
        if (writer == null) {
            Log.i(processTag, logcatMsg);
        } else {
            final ThreadLocals threadLocals = THREAD_LOCALS.get();
            final String threadInfo = formatThreadInfo(threadLocals.threadId, 'I', processTag);
            // write under lock in order to keep messages synchronized across sinks
            synchronized (JavaLogger.class) {
                Log.i(processTag, logcatMsg);
                // get timestamp inside lock so as to make it as precise as possible
                final String timestamp = formatTimestamp(threadLocals.calendar);
                writer.append(timestamp).append(threadInfo).println(logcatMsg);
            }
        }
    }

    public void w(@NonNull String msg) {
        final String logcatMsg = mBracedClassTag.concat(msg);
        final String processTag = sProcessTag;
        final PrintWriter writer = sWriter;
        if (writer == null) {
            Log.w(processTag, logcatMsg);
        } else {
            final ThreadLocals threadLocals = THREAD_LOCALS.get();
            final String threadInfo = formatThreadInfo(threadLocals.threadId, 'W', processTag);
            // write under lock in order to keep messages synchronized across sinks
            synchronized (JavaLogger.class) {
                Log.w(processTag, logcatMsg);
                // get timestamp inside lock so as to make it as precise as possible
                final String timestamp = formatTimestamp(threadLocals.calendar);
                writer.append(timestamp).append(threadInfo).println(logcatMsg);
            }
        }
    }

    public void e(@NonNull String msg) {
        final String logcatMsg = mBracedClassTag.concat(msg);
        final String processTag = sProcessTag;
        final PrintWriter writer = sWriter;
        if (writer == null) {
            Log.e(processTag, logcatMsg);
        } else {
            final ThreadLocals threadLocals = THREAD_LOCALS.get();
            final String threadInfo = formatThreadInfo(threadLocals.threadId, 'E', processTag);
            // write under lock in order to keep messages synchronized across sinks
            synchronized (JavaLogger.class) {
                Log.e(processTag, logcatMsg);
                // get timestamp inside lock so as to make it as precise as possible
                final String timestamp = formatTimestamp(threadLocals.calendar);
                writer.append(timestamp).append(threadInfo).println(logcatMsg);
            }
        }
    }

    public void e(@NonNull String msg, @NonNull Throwable throwable) {
        final String logcatMsg = mBracedClassTag.concat(msg);
        final String processTag = sProcessTag;
        final PrintWriter writer = sWriter;
        if (writer == null) {
            Log.e(processTag, logcatMsg, throwable);
        } else {
            final ThreadLocals threadLocals = THREAD_LOCALS.get();
            final String threadInfo = formatThreadInfo(threadLocals.threadId, 'E', processTag);
            final String throwableMsg = Log.getStackTraceString(throwable);
            // write under lock in order to keep messages synchronized across sinks
            synchronized (JavaLogger.class) {
                Log.e(processTag, logcatMsg);
                // get timestamp inside lock so as to make it as precise as possible
                final String timestamp = formatTimestamp(threadLocals.calendar);
                writer.append(timestamp).append(threadInfo).append(logcatMsg).println(throwableMsg);
            }
        }
    }

    @NonNull
    private static String formatThreadInfo(int threadId, char logLevel, @NonNull String processTag) {
        final int processTagLength = processTag.length();
        final StringBuilder threadInfoBuilder = new StringBuilder(SZ_THREAD_INFO + processTagLength)
                .append(PROCESS_ID)
                .append(' ')
                .append(threadId)
                .append(' ')
                .append(logLevel)
                .append(' ')
                .append(processTag);
        switch (SZ_LOGCAT_TAG - processTagLength) {
            case 8:
                threadInfoBuilder.append("        : ");
                break;
            case 7:
                threadInfoBuilder.append("       : ");
                break;
            case 6:
                threadInfoBuilder.append("      : ");
                break;
            case 5:
                threadInfoBuilder.append("     : ");
                break;
            case 4:
                threadInfoBuilder.append("    : ");
                break;
            case 3:
                threadInfoBuilder.append("   : ");
                break;
            case 2:
                threadInfoBuilder.append("  : ");
                break;
            case 1:
                threadInfoBuilder.append(" : ");
                break;
            default:
                threadInfoBuilder.append(": ");
                break;
        }
        return threadInfoBuilder.toString();
    }

    @NonNull
    private static String formatTimestamp(@NonNull Calendar calendar) {
        calendar.setTimeInMillis(System.currentTimeMillis());
        final StringBuilder timestampBuilder = new StringBuilder(SZ_MSG_TIMESTAMP);
        final int month = calendar.get(Calendar.MONTH);
        if (month < 10) {
            timestampBuilder.append('0');
        }
        timestampBuilder.append(month).append('-');
        final int day = calendar.get(Calendar.DAY_OF_MONTH);
        if (day < 10) {
            timestampBuilder.append('0');
        }
        timestampBuilder.append(day).append(' ');
        final int hour = calendar.get(Calendar.HOUR);
        if (hour < 10) {
            timestampBuilder.append('0');
        }
        timestampBuilder.append(hour).append(':');
        final int minute = calendar.get(Calendar.MINUTE);
        if (minute < 10) {
            timestampBuilder.append('0');
        }
        timestampBuilder.append(minute).append(':');
        final int second = calendar.get(Calendar.SECOND);
        if (second < 10) {
            timestampBuilder.append('0');
        }
        timestampBuilder.append(second).append(':');
        final int millisecond = calendar.get(Calendar.MILLISECOND);
        if (millisecond < 100) {
            timestampBuilder.append('0');
        }
        if (millisecond < 10) {
            timestampBuilder.append('0');
        }
        return timestampBuilder
                .append(millisecond)
                .append(' ')
                .toString();
    }

    /** Stores objects that should be accessed using ThreadLocal objects. */
    private static final class ThreadLocals {
        final Calendar calendar;
        final int threadId;

        ThreadLocals(@NonNull Calendar calendar, int threadId) {
            this.calendar = calendar;
            this.threadId = threadId;
        }
    }
}