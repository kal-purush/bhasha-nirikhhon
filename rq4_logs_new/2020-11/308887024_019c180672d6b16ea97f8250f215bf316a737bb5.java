package com.marketplace.domain;

import com.marketplace.fixtures.FakeCurrencyLookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class ClassifiedAdTest {
    final UUID id = UUID.fromString("53a94b25-8139-4f15-b533-8fcd328c0edd");
    final UUID ownerId = UUID.fromString("c7185cc3-a8db-4030-ace2-59dd773d86fc");
    final UUID approverId = UUID.fromString("3203de96-599f-421a-a9c1-8b365d21cf37");
    private ClassifiedAd classifiedAd;

    @BeforeEach
    public void setup() {
        var classifiedAdId = new ClassifiedAdId(id);
        var classifiedOwnerId = new UserId(ownerId);
        classifiedAd = new ClassifiedAd(classifiedAdId, classifiedOwnerId);
    }

    @Test
    void testCreatedClassified() {
        assertThat(classifiedAd.getId()).isEqualTo(new ClassifiedAdId(id));
        assertThat(classifiedAd.getOwnerId()).isEqualTo(new UserId(ownerId));
        assertThat(classifiedAd.getChanges().size()).isEqualTo(1);
    }

    @Test
    void can_publish_a_valid_ad() {
        classifiedAd.setTitle(new ClassifiedAdTitle("Test ad"));
        classifiedAd.updateText(new ClassifiedAdText("Please buy my stuff"));
        classifiedAd.updatePrice(new Price(Money.fromDecimal(100, "EUR", new FakeCurrencyLookup())));

        classifiedAd.requestToPublish();
        assertThat(classifiedAd.getState()).isEqualTo(ClassifiedAdState.pendingReview);

        assertThat(classifiedAd.getChanges().size()).isEqualTo(5);
    }

    @Test
    void cannot_publish_without_price() {
        assertThatThrownBy(() -> {
            classifiedAd.setTitle(new ClassifiedAdTitle("Test ad"));
            classifiedAd.updateText(new ClassifiedAdText("Please buy my stuff"));

            classifiedAd.requestToPublish();
        }).isInstanceOf(InvalidStateException.class);
    }

    @Test
    void cannot_publish_without_text() {
        assertThatThrownBy(() -> {
            classifiedAd.setTitle(new ClassifiedAdTitle("Test ad"));
            classifiedAd.updatePrice(new Price(Money.fromDecimal(100, "EUR", new FakeCurrencyLookup())));

            classifiedAd.requestToPublish();
        }).isInstanceOf(InvalidStateException.class);
    }

    @Test
    void cannot_publish_without_title() {
        assertThatThrownBy(() -> {
            classifiedAd.updateText(new ClassifiedAdText("Please buy my stuff"));
            classifiedAd.updatePrice(new Price(Money.fromDecimal(100, "EUR", new FakeCurrencyLookup())));

            classifiedAd.requestToPublish();
        }).isInstanceOf(InvalidStateException.class);
    }

    @Test
    void cannot_publish_with_zero_price() {
        assertThatThrownBy(() -> {

            classifiedAd.setTitle(new ClassifiedAdTitle("Test ad"));
            classifiedAd.updateText(new ClassifiedAdText("Please buy my stuff"));
            classifiedAd.updatePrice(new Price(Money.fromDecimal(0, "EUR", new FakeCurrencyLookup())));

            classifiedAd.requestToPublish();
        }).isInstanceOf(InvalidStateException.class);
    }
}