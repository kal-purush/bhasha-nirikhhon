import java.util.ArrayList;


public class Formula {

	ArrayList<Integer> formula = new ArrayList<Integer>();
	ArrayList<Boolean> formulaVals = new ArrayList<Boolean>();
	
	private void removeVariable(int varPos)
	{
		formulaVals.remove(varPos);
		formulaVals.add(varPos, false);
	}
	
	private void removeClause(int varPos)
	{
		boolean stop = false;
		int curPos = varPos;
		for(; !stop && curPos>=0; curPos--)
		{
			if(formula.get(curPos)==0)
			{
				stop=true;
			}
		}
		stop = true;
		if(curPos!=0){
			curPos++;
		}
		for(; !stop && curPos< formula.size(); curPos++)
		{
			formulaVals.remove(curPos);
			formulaVals.add(curPos, false);
			if(formula.get(curPos)==0){
				stop = true;
			}
		}
	}
	
}