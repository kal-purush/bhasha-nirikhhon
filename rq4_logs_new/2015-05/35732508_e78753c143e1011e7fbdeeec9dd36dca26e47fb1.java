package unitTestModelObjects;

import static org.junit.Assert.*;
import modelObjects.BlackjackPlayer;
import modelObjects.BlackjackTable;
import modelObjects.Shoe;

import org.junit.Test;

import exceptions.InvalidShoeException;
import exceptions.TableSeatNumberInvalidException;
import exceptions.TableSeatTakenException;
import rules.BlackjackRules;

public class TestBlackjackTable {
	private Shoe shoe;
	private BlackjackRules.Builder rulesBuilder;
	private BlackjackRules blackjackRules;
	
	private void beforeEach() {
		try {
			shoe = new Shoe(2, 55);
		} catch (InvalidShoeException e) {
			e.printStackTrace();
		}
		rulesBuilder = new BlackjackRules.Builder();
		rulesBuilder.setBlackjackPayoutMultiple(1.5);
		rulesBuilder.setDealerHitsSoft17(false);
		rulesBuilder.setDoubleAfterSplitAllowed(false);
		rulesBuilder.setMaxHandsAfterSplits(4);
		rulesBuilder.setCanResplitAces(false);
		blackjackRules = rulesBuilder.build();
	}
	
	@Test
	public void canAddPlayer() {
		beforeEach();
		BlackjackTable blackjackTable = new BlackjackTable(shoe, blackjackRules);
		
		try {
			blackjackTable.addPlayerAtSeat(new BlackjackPlayer(0, true), 1);
		} catch (TableSeatTakenException | TableSeatNumberInvalidException e) {
			e.printStackTrace();
		}
	}
	
	@Test(expected=TableSeatTakenException.class)
	public void cantAddPlayerSeatTaken() throws TableSeatTakenException, TableSeatNumberInvalidException {
		beforeEach();
		BlackjackTable blackjackTable = new BlackjackTable(shoe, blackjackRules);
		
		blackjackTable.addPlayerAtSeat(new BlackjackPlayer(0, true), 1);
		blackjackTable.addPlayerAtSeat(new BlackjackPlayer(0, true), 1);
	}
	
	@Test(expected=TableSeatNumberInvalidException.class)
	public void cantAddPlayerInvalidSeat() throws TableSeatTakenException, TableSeatNumberInvalidException {
		beforeEach();
		BlackjackTable blackjackTable = new BlackjackTable(shoe, blackjackRules);
		
		blackjackTable.addPlayerAtSeat(new BlackjackPlayer(0, true), -1);
	}

}