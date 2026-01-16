package com.experisproject.experisproject.models.entities;

import javax.persistence.Entity;

@Entity
public class MatchGoal {
	//?? about primary key, can be goalId??? why? how?
	//@Id
	//goalId;
	private String description;

    // goal_id INT NOT NULL,
	//  description VARCHAR(64),
	//  goal_type_id INT NOT NULL,
	//  match_id INT NOT NULL,
	//  player_id INT NOT NULL,
	//  PRIMARY KEY (goal_id),
	//  FOREIGN KEY (goal_type_id) REFERENCES GOAL_TYPE(goal_type_id),
	//  FOREIGN KEY (match_id) REFERENCES MATCH(match_id),
	//  FOREIGN KEY (player_id) REFERENCES PLAYER(player_id)

}