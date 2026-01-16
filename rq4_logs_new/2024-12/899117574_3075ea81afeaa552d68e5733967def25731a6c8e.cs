namespace MortageCalculatorTest
{
	//TODO create a method to validate user input is empty 2024/12/05
	[TestClass]
	public sealed class MortageCalcTest
	{
		[TestMethod]
		public void ValidateUserInputNumberic()
		{
			// arrange

			// act
			string input = 
			decimal result = default(decimal);
			// assert

			Assert.IsTrue(decimal.TryParse(input, out decimal result));

		}


		[TestMethod]
		public void ValidateUserInputNull()
		{
			// arrange

			// act
			string input =

			// assert

			Assert.IsNull(input);
			

		}

		[TestMethod]
		public void MortageFormulaOutput()
		{

		}
	}
}
