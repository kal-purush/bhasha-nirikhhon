package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.query.results.AbstractResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class QueueResults extends AbstractResults implements Results {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(QueueResults.class);
    public static final @Nonnull Solution DEFAULT_END
            = ArraySolution.forVars(Collections.singletonList("empty"))
                           .fromValues(StdLit.fromEscaped("empty"));

    private final @Nonnull BlockingQueue<? extends Solution> queue;
    private @Nullable Runnable afterConsume;
    private @Nullable ArraySolution.ValueFactory projector = null;
    private final @Nonnull List<Object> onClose = new ArrayList<>();
    private boolean closed = false;
    private final @Nonnull Solution endSolution;
    private @Nullable Solution next;

    public QueueResults(@Nonnull Collection<String> varNames,
                        @Nonnull BlockingQueue<? extends Solution> queue) {
        this(varNames, queue, DEFAULT_END);
    }

    public QueueResults(@Nonnull Collection<String> varNames,
                        @Nonnull BlockingQueue<? extends Solution> queue,
                        @Nonnull Solution endSolution) {
        super(varNames);
        this.queue = queue;
        this.endSolution = endSolution;
    }

    public @Nullable Runnable afterConsume(@Nonnull Runnable r) {
        Runnable old = this.afterConsume;
        this.afterConsume = r;
        return old;
    }

    public @Nonnull QueueResults onClose(@Nonnull Runnable r) {
        onClose.add(r);
        return this;
    }
    public @Nonnull QueueResults onClose(@Nonnull Callable<?> callable) {
        onClose.add(callable);
        return this;
    }
    public @Nonnull QueueResults onClose(@Nonnull AutoCloseable closeable) {
        onClose.add(closeable);
        return this;
    }

    public @Nonnull Solution getEndSolution() {
        return endSolution;
    }

    public @Nonnull BlockingQueue<? extends Solution> getQueue() {
        return queue;
    }

    private @Nonnull Solution project(@Nonnull Solution solution) {
        if (afterConsume != null)
            afterConsume.run();
        if (solution != endSolution && !getVarNames().equals(solution.getVarNames())) {
            if (projector == null)
                projector = ArraySolution.forVars(getVarNames());
            return projector.fromSolution(solution);
        }
        return solution;
    }

    @Override public boolean hasNext() {
        boolean interrupted = false;
        try {
            while (next == null) {
                try {
                    Solution solution = queue.take();
                    next = project(solution);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            return next != endSolution;
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    @Override public boolean hasNext(int millisecondsTimeout) {
        boolean interrupted = false;
        try {
            while (next == null) {
                try {
                    Solution solution = queue.poll(millisecondsTimeout, TimeUnit.MILLISECONDS);
                    if (solution != null)
                        next = project(solution);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            return next != endSolution;
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    @Override public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        Solution next = this.next;
        assert next != null;
        this.next = null;
        return next;
    }

    @Override public void close() throws ResultsCloseException {
        if (closed)
            return;
        closed = true;
        List<Throwable> ts = null;
        for (Object o : onClose) {
            try {
                if (o instanceof Runnable)
                    ((Runnable)o).run();
            } catch (Throwable t) {
                (ts == null ? ts = new ArrayList<>() : ts).add(t);
            }
            try {
                if (o instanceof Callable)
                    ((Callable<?>) o).call();
            } catch (Throwable t) {
                (ts == null ? ts = new ArrayList<>() : ts).add(t);
            }
            try {
                if (o instanceof AutoCloseable)
                    ((AutoCloseable) o).close();
            } catch (Throwable t) {
                (ts == null ? ts = new ArrayList<>() : ts).add(t);
            }
        }
        if (ts != null) {
            for (int i = 1, len = ts.size(); i < len; i++)
                ts.get(0).addSuppressed(ts.get(i));
            throw new ResultsCloseException(this, ts.get(0));
        }
    }
}