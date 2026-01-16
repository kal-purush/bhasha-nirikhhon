package com.minesweeper.api.rules;

import org.jeasy.rules.annotation.Action;
import org.jeasy.rules.annotation.Condition;
import org.jeasy.rules.annotation.Fact;
import org.jeasy.rules.annotation.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.minesweeper.api.domain.Game;
import com.minesweeper.api.domain.Locker;
import com.minesweeper.api.domain.LockerRequest;

@Rule(name = "MarkFlagLocker", description = "Mark a flag in the given locker")
public class MarkFlagLocker {
	
	private final Logger log = LoggerFactory.getLogger(MarkFlagLocker.class);
	
	@Condition
	public boolean canMarkQuestionLocker(@Fact("request") LockerRequest lockerReq, @Fact("game") Game game) {
		return !game.getLockers().get(lockerReq.getY()).get(lockerReq.getX()).isExposed() && lockerReq.isFlag();
	}
	
	@Action
	public void markQuestionLocker(@Fact("request") LockerRequest lockerReq, @Fact("game") Game game) {
		Locker locker = game.getLockers().get(lockerReq.getY()).get(lockerReq.getX());
		locker.setFlag(Boolean.TRUE);
		locker.setQuestion(Boolean.FALSE);
		game.getLockers().get(lockerReq.getY()).set(lockerReq.getX(), locker);
	}

}