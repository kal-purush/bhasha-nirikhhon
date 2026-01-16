package com.marketplace.domain;

import com.marketplace.event.Event;
import com.marketplace.event.EventId;
import com.marketplace.framework.Entity;
import com.marketplace.framework.EventApplier;
import lombok.Getter;

@Getter
public class Picture extends Entity<EventId, Event> {
    private PictureId id;
    private ClassifiedAdId parentId;
    private PictureSize size;
    private String uri;
    private int order;

    public Picture(EventApplier<Event> applier) {
        super(applier);
    }

    @Override
    public void when(Event event) {

    }

    public void resize(PictureSize newSize) {

    }
}