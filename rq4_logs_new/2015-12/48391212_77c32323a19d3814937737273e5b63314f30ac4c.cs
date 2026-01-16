using System;

namespace Calculator
{
	public class Function : Operand
	{
		public Function (Operand lhs, Operator op, Operand rhs)
		{
			LeftHandSide = lhs;
			Operator = op;
			RightHandSide = rhs;
		}

		public void Calculate()
		{
			throw new NotImplementedException ();
			SetResult (5);
		}

		public Operand LeftHandSide { get; private set;}

		public Operator Operator { get; private set;}

		public Operand RightHandSide { get; private set;}
	}

	public enum Operator
	{
		divide,
		multiply,
		add,
		subtract,
	}
}
