package com.codeup.mentor.model;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name="messages")
public class Message {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(nullable = false)
    private long id;

    @Column(nullable = false, length = 750)
    private String body;

    @Column(nullable = false)
    private LocalDateTime datetime;

    @ManyToOne
    @JoinColumn(name="sender_id")
    private User sender_info;

    @ManyToOne
    @JoinColumn(name="recever_id")
    private User receiver_info;


}