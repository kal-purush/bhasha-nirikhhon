package ru.otus.batchConfig.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemProcessListener;

public class BatchProcessListener implements ItemProcessListener {

    private Logger logger = LoggerFactory.getLogger(BatchProcessListener.class);

    @Override
    public void beforeProcess(Object o) {
        logger.info("Before convert {} class {}", o, o.getClass());
    }

    @Override
    public void afterProcess(Object o, Object o2) {
        logger.info("After convert from {} class {} to {} class {}", o, o.getClass(), o2, o2.getClass());
    }

    @Override
    public void onProcessError(Object o, Exception e) {

    }
}