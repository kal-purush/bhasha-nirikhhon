package tososomaru.wb.ads.common.base;

import tososomaru.wb.ads.common.base.DomainEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public abstract class Entity {

    protected final UUID id;
    private final List<DomainEvent> domainEvents = new ArrayList<>();

    protected Entity() {
        this.id = UUID.randomUUID();
    }

    protected Entity(UUID id) {
        this.id = id;
    }

    public UUID getId() {
        return id;
    }

    public void addDomainEvent(DomainEvent event) {
        domainEvents.add(event);
    }

    public void removeDomainEvent(DomainEvent event) {
        domainEvents.remove(event);
    }

    /**
     * Returns an unmodifiable list of events
     * @return
     */
    public List<DomainEvent> getDomainEvents() {
        return Collections.unmodifiableList(domainEvents);
    }
}