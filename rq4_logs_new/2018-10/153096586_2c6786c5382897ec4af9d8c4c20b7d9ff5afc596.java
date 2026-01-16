package com.experisproject.experisproject.models.entities;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class Team {
	@Id
	@GeneratedValue
	private int teamId;

	//  owner_id INT NOT NULL,
	//  association_id INT NOT NULL,
	//  coach_id INT NOT NULL,
	//  location_id INT NOT NULL,
	//  PRIMARY KEY (team_id),
	//  FOREIGN KEY (owner_id) REFERENCES OWNER(owner_id),
	//  FOREIGN KEY (association_id) REFERENCES ASSOCIATION(association_id),
	//  FOREIGN KEY (coach_id) REFERENCES COACH(coach_id),
	//  FOREIGN KEY (location_id) REFERENCES LOCATION(location_id)


	public Team() {
	}
}