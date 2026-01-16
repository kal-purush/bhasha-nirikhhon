import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BullsCowsGame <T> {
	
	public static final int DEFAULT_COMBINATION_LENGTH = 4;
	
	private List<T> winningCombination;
	private int combinationLength;
	private boolean isGameOver = false;
	
	public BullsCowsGame() {
		this(DEFAULT_COMBINATION_LENGTH);
	}
	
	public BullsCowsGame(int combinationLength){
		this.combinationLength = combinationLength;
		winningCombination = (List<T>) generateIntegerCombination();
		
	}

	public BullsCowsGame(List<T> winningCombination) {
		this.combinationLength = combinationLength;
		this.winningCombination = new ArrayList<>(winningCombination);
	}

	public boolean isOver() {
		return isGameOver;
	}
	
	public CombinationResult evaluateCombination(List<T> combination) throws InvalidCombinationException{
	
		if(combination.size() != this.combinationLength){
			throw new InvalidCombinationException("ERROR: Invalid combination length!");
		}

		int bulls = 0, cows = 0;

		for (int i = 0; i < combination.size(); i++) {
			if(combination.get(i).equals(winningCombination.get(i)))
			{
				bulls++;
			} else if (winningCombination.contains(combination.get(i))){
				cows++;
			}
		}
		
		if(bulls == combinationLength){
			isGameOver = true;
		}

		return new CombinationResult(bulls, cows);
	}
	
	private List<Integer> generateIntegerCombination(){
		List<Integer> numbers = Arrays.asList(0,1,2,3,4,5,6,7,8,9);
		Collections.shuffle(numbers);
		
		List<Integer> generatedNumber = new ArrayList<>();
		for (int i = 0; i < this.combinationLength; i++) {
			generatedNumber.add(numbers.get(i));
		}
		
		return generatedNumber;
	}

}