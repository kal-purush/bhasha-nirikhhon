package com.minesweeper.api.rules;

import java.awt.Point;

import org.jeasy.rules.annotation.Action;
import org.jeasy.rules.annotation.Condition;
import org.jeasy.rules.annotation.Fact;
import org.jeasy.rules.annotation.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.minesweeper.api.domain.Game;
import com.minesweeper.api.domain.LockerRequest;
import com.minesweeper.api.domain.enume.GameStatus;

@Rule(name = "CheckBombLocker", description = "Check if a given Point has a bomb in the locker")
public class CheckBombLocker {
	
	private final Logger log = LoggerFactory.getLogger(CheckBombLocker.class);
	
	@Condition
	public boolean isBombLocker(@Fact("request") LockerRequest lockerRequest, @Fact("game") Game game) {
		return lockerRequest.isExposed();
	}
	
	@Action
	public void showBlankNeighboringLockers(@Fact("request") LockerRequest lockerRequest, @Fact("game") Game game) throws Exception {
		int x = lockerRequest.getX();
		int y = lockerRequest.getY();
		Point point = new Point(x, y);
		if (game.getMinesLocations().stream().filter(l -> {
			log.info("CheckBomb: {} - {}", l.getPoint().toString(), point.toString());
			return l.getPoint().equals(point);
		}).findAny().isPresent()) {
			game.setGameStatus(GameStatus.LOST);
			//TODO game.getLockers().get(y).set(x, element)
			throw new Exception("Game over");
		}
	}

}