package com.worthent.foundation.util.state.provider;

import com.worthent.foundation.util.annotation.NotNull;
import com.worthent.foundation.util.annotation.Nullable;
import com.worthent.foundation.util.state.StateEvent;
import com.worthent.foundation.util.state.StateExeException;
import com.worthent.foundation.util.state.StateTable;
import com.worthent.foundation.util.state.StateTableControl;
import com.worthent.foundation.util.state.StateTableData;
import com.worthent.foundation.util.state.impl.StateEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static com.worthent.foundation.util.condition.Preconditions.checkNotNull;

/**
 * Implements the {@link StateTableControl} interface to provide a single-threaded implementation of the state table.
 * The state table transition actions are all conducted asynchronously on the same single thread in the order they arrive.
 *
 * @author Erik K. Worth
 */
public class SingleThreadConsumerStateTableControl<D extends StateTableData, E extends StateEvent>
        implements StateTableControl<E>, Closeable {

    /** Logger for this class */
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleThreadConsumerStateTableControl.class);

    /** Exception message when attempting to submit an event when the state table is shut down */
    public static final String MSG_STATE_TABLE_SHUT_DOWN = "State Table is shut down";

    /** The default thread group name when a thread group is not provided in the constructor */
    private static final String DEFAULT_THREAD_GROUP_NAME = "single-thread-state-table";

    /** Counts the number of instances of this class to use as the name of the single thread for this state table */
    private static final AtomicInteger instance_count = new AtomicInteger(0);

    /** The state table engine that processes events */
    private final StateEngine<D, E> engine;

    /** The state table instance */
    private final StateTable<D, E> stateTblInstance;

    /** The event queue */
    private final LinkedBlockingDeque<E> eventQueue;

    /** Set to <code>true</code> when the state table is stopping */
    private volatile boolean stopping = false;

    /** The single thread used to process all events */
    private Thread thread;

    /**
     * Construct with the state table instance and an optional thread group for the single thread.
     *
     * @param stateTblInstance the state table to be fed events from the single thread in the order the events are signaled
     * @param threadGroup the optional thread group
     */
    public SingleThreadConsumerStateTableControl(
            @NotNull final StateTable<D, E> stateTblInstance,
            @Nullable final ThreadGroup threadGroup) {
        final ThreadGroup threadGrp = (null == threadGroup)
                ? new ThreadGroup(DEFAULT_THREAD_GROUP_NAME)
                : threadGroup;
        this.engine = new StateEngine<>();
        this.stateTblInstance = checkNotNull(stateTblInstance, "stateTblInstance must not be null");
        this.thread = new Thread(
                threadGroup,
                this::processEvents,
                threadGrp.getName() + '-' + instance_count.incrementAndGet());
        this.thread.setDaemon(true); // do not prevent the process from shutting down
        this.eventQueue = new LinkedBlockingDeque<>();
    }

    /**
     * Construct with the state table instance.
     *
     * @param stateTblInstance the state table to be fed events from the single thread in the order the events are signaled
     */
    public SingleThreadConsumerStateTableControl(@NotNull final StateTable<D, E> stateTblInstance) {
        this(stateTblInstance, null);
    }

    //
    // Closeable Interface
    //

    @Override
    public void close() throws IOException {
        stop();
    }

    //
    // StateTableControl Interface
    //

    @Override
    public void start() throws StateExeException {
        try {
            stateTblInstance.getStateTableDataManager().initializeStateTableData();
        } catch (Exception exc) {
            final String name = stateTblInstance.getStateTableName();
            throw new StateExeException("Error initializing state table history for state table, " + name);
        }
        thread.start();
    }

    @Override
    public void stop() throws StateExeException {
        stopping = true;
        thread.interrupt();
    }

    @Override
    public void signalEvent(@NotNull final E event) throws StateExeException {
        checkNotNull(event, "event must not be null");
        if (!thread.isAlive()) {
            throw new StateExeException(MSG_STATE_TABLE_SHUT_DOWN);
        }
        eventQueue.add(event);
    }

    /**
     * Injects an event into the front of the queue submitted to the state table.
     *
     * @param event the event to inject into the front of the queue
     */
    public void injectEvent(@NotNull final E event) throws StateExeException {
        checkNotNull(event, "event must not be null");
        if (!thread.isAlive()) {
            throw new StateExeException(MSG_STATE_TABLE_SHUT_DOWN);
        }
        eventQueue.addFirst(event);
    }

    /** The method run from within the single thread that processes events */
    private void processEvents() {
        while (!stopping) {
            E event = null;
            try {
                event = eventQueue.take();
                if (null != event) {
                    LOGGER.debug("Process event: {}", event);
                    engine.processEvent(stateTblInstance, this, event);
                }
            } catch (final InterruptedException exc) {
                LOGGER.info("State Table Thread interrupted");
            } catch (final Exception exc) {
                LOGGER.error("Error processing event " + event, exc);
            }
        }
        LOGGER.info("State Table thread has stopped.");
    }

}