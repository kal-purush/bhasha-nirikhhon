package com.example.demo.domain;

import java.util.List;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.OneToOne;

@Entity
public class Matching {

    @Id @GeneratedValue
    private Long id;

    @OneToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "condition_id")
    private MatchingCondition condition;

    @ManyToMany
    private List<User> matchedUsers;

    @OneToOne(mappedBy = "matching")
    private ChatRoom chatRoom;
}