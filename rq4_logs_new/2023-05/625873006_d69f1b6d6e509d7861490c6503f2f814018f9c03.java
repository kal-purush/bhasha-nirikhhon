package ru.practicum.main.model.comment;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import ru.practicum.main.model.event.Event;

import javax.persistence.*;

@Entity
@Builder
@Table(name = "admin_event_comments")
@Getter
@AllArgsConstructor
@NoArgsConstructor
@NamedEntityGraph(
        name = "admin_comment-events-graph-requester",
        attributeNodes = {
                @NamedAttributeNode(value = "event", subgraph = "subgraph-event")
        },
        subgraphs = {
                @NamedSubgraph(name = "subgraph-event",
                        attributeNodes = {
                                @NamedAttributeNode(value = "initiator"),
                                @NamedAttributeNode(value = "category")
                        })
        })
public class AdminEventComment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    @ManyToOne
    @JoinColumn(name = "event_id")
    Event event;

    String text;
}