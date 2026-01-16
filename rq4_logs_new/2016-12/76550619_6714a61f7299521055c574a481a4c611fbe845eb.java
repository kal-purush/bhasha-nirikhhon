package com.hackathon.speakers.util.thread;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutors {
    protected static final Logger logger = LoggerFactory.getLogger(TaskExecutors.class);

    public static final TaskExecutor speak = create("Speak", 1, 10, 30, Thread.MAX_PRIORITY);

    public static TaskExecutor create(String executorName, int poolSize, int warningQueueSize, int criticalQueueSize,
            int threadPriority) {
        return create(executorName, poolSize, warningQueueSize, criticalQueueSize, threadPriority,
                new QueueStateCallback(executorName));
    }

    public static TaskExecutor create(String executorName, int poolSize, int warningQueueSize, int criticalQueueSize,
            int threadPriority, QueueStateCallback queueStateCallback) {
        TaskExecutor executor = new TaskExecutor(executorName, poolSize, warningQueueSize, criticalQueueSize,
                threadPriority, queueStateCallback);

        return executor;
    }

    public static void shutdownNow() {
        List<Runnable> runnables = speak.shutdownNow();

        if (!runnables.isEmpty()) {
            logger.warn("Executor {} had {} tasks that didn't run: {}", speak.getName(), runnables.size(), runnables);
        }
    }

    public static boolean checkHealth() {
        if (speak.isCritical()) {
            return false;
        }

        return true;
    }

    private static class QueueStateCallback implements TaskExecutor.QueueStateCallback {
        private final String executorName;

        public QueueStateCallback(String executorName) {
            this.executorName = executorName;
        }

        @Override
        public void onWarning(TaskExecutor target, int queueSize) {
            logWarning("WARNING", queueSize);
        }

        @Override
        public void onCritical(TaskExecutor target, int queueSize) {
            logWarning("CRITICAL", queueSize);
        }

        private void logWarning(String title, int queueSize) {
            logger.warn("[{}] Task executor \"{}\" has {} tasks queued.", title, executorName, queueSize);
        }
    }
}