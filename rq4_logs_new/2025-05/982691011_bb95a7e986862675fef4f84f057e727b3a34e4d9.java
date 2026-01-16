package com.capgemini.Voting_system.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Candidates {
	@Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "candidate_id")
	private Long candidateId;
	@ManyToOne
    @JoinColumn(name = "party_id", referencedColumnName = "party_id")
    private Party party;
    
    @ManyToOne
    @JoinColumn(name = "user_id", referencedColumnName = "user_id")
    private User user;
	private String manifesto;
	@ManyToOne
    @JoinColumn(name = "election_id", referencedColumnName = "election_id")
    private Election election;
	private Long electionId;
}