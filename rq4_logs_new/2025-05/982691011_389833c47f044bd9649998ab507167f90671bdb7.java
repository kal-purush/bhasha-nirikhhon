package com.capgemini.Voting_system.entities;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "party")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Party {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "party_id")
    private Long partyId;

    @Column(name = "party_name")
    private String partyName;

    @Column(name = "party_status")
    private String partyStatus;

    @Column(name = "party_logo")
    private String partyLogo;
}