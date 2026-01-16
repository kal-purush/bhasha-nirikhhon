package com.example.hello.impl;

import akka.Done;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.lightbend.lagom.javadsl.persistence.AggregateEventTag;
import com.lightbend.lagom.javadsl.persistence.ReadSideProcessor;
import com.lightbend.lagom.javadsl.persistence.cassandra.CassandraReadSide;
import com.lightbend.lagom.javadsl.persistence.cassandra.CassandraSession;

import org.pcollections.PSequence;
import org.pcollections.TreePVector;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static com.lightbend.lagom.javadsl.persistence.cassandra.CassandraReadSide.completedStatement;

public class HelloEventProcessor  extends ReadSideProcessor<HelloEvent> {

    private final CassandraSession session;
    private final CassandraReadSide readSide;

    private PreparedStatement writeGreetings = null; // initialized in prepare


    @Inject
    public HelloEventProcessor(CassandraSession session, CassandraReadSide readSide) {
        this.session = session;
        this.readSide = readSide;
    }

    private void setWriteGreetings(PreparedStatement writeGreetings) {
        this.writeGreetings = writeGreetings;
    }

    @Override
    public ReadSideHandler<HelloEvent> buildHandler() {
        return readSide.<HelloEvent>builder("hello_offset")
                .setGlobalPrepare(this::prepareCreateTables)
                .setPrepare((ignored) -> prepareWriteGreetings())
                .setEventHandler(HelloEvent.GreetingMessageChanged.class, this::processGreetingMessageChanged)
                .build();
    }
    
    @Override
    public PSequence<AggregateEventTag<HelloEvent>> aggregateTags() {
        return TreePVector.singleton(HelloEventTag.INSTANCE);
    }

    private CompletionStage<Done> prepareCreateTables() {
        // @formatter:off
        return session.executeCreateTable(
                "CREATE TABLE IF NOT EXISTS greeting ("
                        + "userId text, message text, "
                        + "PRIMARY KEY (userId, message))");
        // @formatter:on
    }

    private CompletionStage<Done> prepareWriteGreetings() {
        return session.prepare("INSERT INTO greeting (userId, message) VALUES (?, ?)").thenApply(ps -> {
            setWriteGreetings(ps);
            return Done.getInstance();
        });
    }

    private CompletionStage<List<BoundStatement>> processGreetingMessageChanged(HelloEvent.GreetingMessageChanged event) {
        BoundStatement bindWriteGreetings = writeGreetings.bind();
        bindWriteGreetings.setString("userId", event.userId);
        bindWriteGreetings.setString("message", event.message);
        return completedStatement(bindWriteGreetings);
    }
}