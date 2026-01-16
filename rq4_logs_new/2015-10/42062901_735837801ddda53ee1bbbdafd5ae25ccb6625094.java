package coverage.expressions;

public class ExpressionGreaterOrEqualsThanValue implements Expression 
{
	private int x;
	private int value;
	
	public ExpressionGreaterOrEqualsThanValue(int x, int value) 
	{
		this.x = x;
		this.value = value;
	}

	@Override
	public boolean eval() 
	{
		return x >= value;
	}

	@Override
	public String getPredicate() 
	{
		return "x>=" + value;
	}
}