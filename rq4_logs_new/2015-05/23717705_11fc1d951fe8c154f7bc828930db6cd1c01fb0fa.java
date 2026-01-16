package agent;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import game.Board;
import game.Group;
import game.Point;

public class DivideGroupsScoring implements IScoreMethod {

	public int score(final Board board, final Point move) {
		
		final Set<Group> enemyGroups = board.getGroups().stream()
				.filter(g -> g.getColor() != board.getToMove())
				.collect(Collectors.toSet());

		int score = (int) Math.round(10 * Math.random());
		Set<List<Group>> enemyGroupCombinations = 
				Sets.cartesianProduct(enemyGroups, enemyGroups).stream()
				.filter(l -> l.get(0) != l.get(1))
				.collect(Collectors.toSet());
		
		for(final List<Group> l : enemyGroupCombinations) {
			
			final Group a = l.get(0);
			final Group b = l.get(1);
			
			final Point pA = a.getClosestPoint(move);
			final Point pB = b.getClosestPoint(move);
			final Point pC = a.getClosestPoint(pB);
			
			int groupDist = pB.distanceFrom(pC);
			int smallerDist = Math.min(move.distanceFrom(pA), move.distanceFrom(pB));
			int largerDist = Math.max(move.distanceFrom(pA), move.distanceFrom(pB));
			
			score += Math.round((smallerDist * 100) / largerDist);
			score += Math.round((groupDist * 100) / (smallerDist+largerDist));
			
		}
		
		return score;
	}

}