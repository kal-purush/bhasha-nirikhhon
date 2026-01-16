package com.marketplace.domain;

import com.marketplace.framework.events.*;
import com.marketplace.framework.Entity;

public class ClassifiedAd extends Entity<EventId> {
    private ClassifiedAdId id;
    private UserId ownerId;
    private ClassifiedAdTitle title;
    private ClassifiedAdText text;
    private Price price;
    private UserId approvedBy;
    private ClassifiedAdState state;

    public ClassifiedAd(ClassifiedAdId id, UserId ownerId) {
        apply(new ClassifiedAdCreated(id.id(), ownerId.id()));
    }

    public void setTitle(ClassifiedAdTitle title) {
        apply(new ClassifiedAdTitleChanged(id.id(), title.toString()));
    }

    public void updateText(ClassifiedAdText text) {
        apply(new ClassifiedAdTextUpdated(id.id(), text.toString()));
    }

    public void updatePrice(Price price) {
        apply(new ClassifiedAdPriceUpdated(id.id(), price.money().amount(), price.money().currencyCode()));
    }

    public void requestToPublish() {
        apply(new ClassifiedAdSentForReview(id.id()));
    }

    public ClassifiedAdId getId() {
        return id;
    }

    public UserId getOwnerId() {
        return ownerId;
    }

    public ClassifiedAdTitle getTitle() {
        return title;
    }

    public ClassifiedAdText getText() {
        return text;
    }

    public Price getPrice() {
        return price;
    }

    public UserId getApprovedBy() {
        return approvedBy;
    }

    public ClassifiedAdState getState() {
        return state;
    }

    @Override
    public void when(Event event) {
        this.id = new ClassifiedAdId(event.getId());
        if (event instanceof ClassifiedAdCreated e) {
            this.ownerId = new UserId(e.getUserId());
            this.state = ClassifiedAdState.inactive;
        } else if (event instanceof ClassifiedAdTextUpdated e) {
            this.text = new ClassifiedAdText(e.getAdText());
        } else if (event instanceof ClassifiedAdPriceUpdated e) {
            this.price = new Price(new Money(e.getPrice(), e.getCurrencyCode()));
        } else if (event instanceof ClassifiedAdTitleChanged e) {
            this.text = new ClassifiedAdText(e.getTitle());
        } else if (event instanceof ClassifiedAdSentForReview e) {
            this.state = ClassifiedAdState.pendingReview;
        }
    }

    @Override
    public void ensureValidState() {
        var valid = id != null && ownerId != null;
        valid = valid && switch (this.state) {
            case pendingReview -> title != null && text != null && price != null && price.money() != null && price.money().amount().doubleValue() > 0;
            case inactive, markedAsSold -> true;
            case active -> title != null && text != null &&
                    price != null && price.money() != null &&
                    price.money().amount().doubleValue() > 0 && approvedBy != null;
        };
        if (!valid) {
            throw new InvalidStateException("post checks failed in state");
        }
    }
}