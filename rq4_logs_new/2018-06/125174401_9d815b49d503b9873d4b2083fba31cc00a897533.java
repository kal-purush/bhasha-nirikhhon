package com.mercateo.eventstore.domain;

import com.mercateo.common.UnitTest;
import com.mercateo.eventstore.data.EventInitiatorData;
import com.mercateo.eventstore.example.TestData;
import lombok.val;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;

@Category(UnitTest.class)
public class EventInitiatorTest {

    @Test
    public void mapsInitiator() {
        Reference reference = TestData.EVENT_INITIATOR_WITH_AGENT;

        val initiator = EventInitiator.of(reference);

        assertThat(initiator.id()).isEqualTo(reference.id());
        assertThat(initiator.type()).isEqualTo(reference.type());
        assertThat(initiator.agent()).isEmpty();

    }

    @Test
    public void mapsInitiatorData() {
        val initiatorData = EventInitiatorData.of(TestData.EVENT_INITIATOR_WITH_AGENT);

        val initiator = EventInitiator.of(initiatorData);

        assertThat(initiator.id()).isEqualTo(initiatorData.id());
        assertThat(initiator.type()).isEqualTo(initiatorData.type());
        assertThat(initiator.agent()).contains(initiatorData.agent().get());
    }
}