package com.experisproject.experisproject.models.entities;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class Player {
	@Id
	@GeneratedValue
	private int playerId;

	private String normalPosition;
	private String number;



//  player_id INT NOT NULL,
//  normal_position VARCHAR(64),
//  number VARCHAR(8), (int????)
//  person_id INT NOT NULL,
//  team_id INT NOT NULL,
//  PRIMARY KEY (player_id),
//  FOREIGN KEY (person_id) REFERENCES PERSON(person_id),
//  FOREIGN KEY (team_id) REFERENCES TEAM(team_id)


	public Player() {
	}

	public Player(String normalPosition, String number) {
		this.normalPosition = normalPosition;
		this.number = number;
	}
}