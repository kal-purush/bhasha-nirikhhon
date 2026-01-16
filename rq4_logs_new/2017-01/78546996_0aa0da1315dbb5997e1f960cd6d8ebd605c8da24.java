package com.github.jmcampanini.hrm.emulator;

import com.github.jmcampanini.hrm.emulator.impl.ArrayListOutbox;
import com.github.jmcampanini.hrm.emulator.impl.DefaultProcessor;
import com.github.jmcampanini.hrm.emulator.impl.DefaultWorker;
import com.github.jmcampanini.hrm.emulator.impl.FixedLengthInbox;
import org.immutables.value.Value;

/**
 * Represents the HMR CPU.
 */
@Value.Immutable
public interface Cpu {

    @Value.Default
    default Inbox inbox() {
        return FixedLengthInbox.empty();
    }

    @Value.Default
    default Outbox outbox() {
        return new ArrayListOutbox();
    }

    @Value.Default
    default Worker worker() {
        return new DefaultWorker();
    }

    @Value.Default
    default Processor processor() {
        return new DefaultProcessor();
    }
}