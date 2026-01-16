package com.experisproject.experisproject.models.entities;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.time.LocalDate;

@Entity
public class Match {
	@Id
	@GeneratedValue
	private int matchId;

	private LocalDate matchDate;

    //  season_id INT NOT NULL,
	//  location_id INT NOT NULL,
	//  home_team_id INT NOT NULL,
	//  away_team_id INT NOT NULL,
	//  PRIMARY KEY (match_id),
	//  FOREIGN KEY (season_id) REFERENCES SEASON(season_id),
	//  FOREIGN KEY (location_id) REFERENCES LOCATION(location_id),
	//  FOREIGN KEY (home_team_id) REFERENCES TEAM(team_id),
	//  FOREIGN KEY (away_team_id) REFERENCES TEAM(team_id)
}