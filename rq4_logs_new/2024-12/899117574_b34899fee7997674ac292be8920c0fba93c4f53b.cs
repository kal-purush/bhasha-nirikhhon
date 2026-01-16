using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System

namespace MortgageCalculator
{
	internal class Bank
	{
		MortgageCalc mortgageCalc = new MortgageCalc(10000m, 5m, 30);
		LoanInformation loanInformation = new LoanInformation(10000m, 5m, 30);
		

		public decimal CalculateTotalInterest()
		{
			return mortgageCalc.LoanInformation.LoanAmount * Math.Pow((double)(1 + mortgageCalc.LoanInformation.InterestRate), (mortgageCalc.LoanInformation.LoanDuration / 12) - mortgageCalc.LoanInformation.LoanAmount;
		}
	}
}