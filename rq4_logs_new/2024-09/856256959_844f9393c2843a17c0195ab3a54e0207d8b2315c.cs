using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Popups;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;

// The Blank Page item template is documented at https://go.microsoft.com/fwlink/?LinkId=234238

namespace Calculator
{
	/// <summary>
	/// An empty page that can be used on its own or navigated to within a Frame.
	/// </summary>
	public sealed partial class MortgageCalculator : Page
	{
		public MortgageCalculator()
		{
			this.InitializeComponent();
		}

		// Create a Navigation back to the main calculator menu
		private void btnReturnToNavPage_Click(object sender, RoutedEventArgs e)
		{
			Frame.Navigate(typeof(NavigationPage));
		}

		// Create a method for the Calculation function to generate the outputs to pass the results back to the screen
		private async void btnCalculate_Click(object sender, RoutedEventArgs e)
		{
			// Check if items are the correct data types
			bool principalCheck = int.TryParse(principalBorrowValue.Text, out int principal);
			bool yearsBorrowsCheck = int.TryParse(yearsValue.Text, out int years);
			bool andMonthsCheck = int.TryParse(andMonthsValue.Text, out int months) || string.IsNullOrWhiteSpace(andMonthsValue.Text);
			bool yearlyInterestRateCheck = double.TryParse(yearlyInterestValue.Text, out double InterestRate);
			bool andMonthsCheckblank = string.IsNullOrWhiteSpace(andMonthsValue.Text);


			if (principalCheck && yearsBorrowsCheck && andMonthsCheck && yearlyInterestRateCheck)
			{   // check if months is blank, and if yes, set it to 0 to prevent any validation issues
				if (andMonthsCheckblank == true)
					andMonthsValue.Text = 0.ToString();

				// Take the interest rate entered and convert it to a percentage value
				double monthlyInterestRate = (InterestRate / 100) / 12;

				// Work out how many months there are for the total period of years and months combined
				double totalMonths = (years * 12) + months;

				double monthlyRepayment;

				if (monthlyInterestRate == 0)
				{
					monthlyInterestValue.Text = monthlyInterestRate.ToString();
					monthlyRepayment = principal / totalMonths;
				}
				else
				{
					// Output the monthly interest to Monthly Interest Text Box
					// Interest Calculation
					double interestcalc = Math.Pow(1 + monthlyInterestRate, totalMonths);

					// Monthly Repayment
					double compoundInterest = monthlyInterestRate * interestcalc;
					monthlyInterestValue.Text = monthlyInterestRate.ToString("F5") + " %";
					double monthlyPayment = principal * (compoundInterest / (interestcalc - 1));
					monthlyRepaymentValue.Text = "$ " + monthlyPayment.ToString("F2");
				}
			}
			// display error because incorrect values have been entered
			else
			{
				var errorMsg = new MessageDialog("Invalid Entry: Please enter values");
				await errorMsg.ShowAsync();
				return;
			}
		}
	}
}