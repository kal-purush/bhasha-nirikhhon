package ch.uzh.ifi.access.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class GracefulShutdown {

    private static final Logger logger = LoggerFactory.getLogger(GracefulShutdown.class);

    private boolean isShutdown = false;

    @EventListener(ContextClosedEvent.class)
    public void rejectSubmissionsOnShutdown() {
        logger.warn("Received shutdown signal. Will reject new submissions");
        this.isShutdown = true;
    }

    public boolean isShutdown() {
        return isShutdown;
    }
}