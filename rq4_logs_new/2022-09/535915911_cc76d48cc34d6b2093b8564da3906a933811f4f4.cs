using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Foundation;
using Windows.Foundation.Collections;
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


		private void CalculateButton_Click(object sender, RoutedEventArgs e)
		{
			double principalBorrrow, yearlyIntrestRate, monthlyIntrestRate, monthlyRepayment, noOfMonths, years, months;
			principalBorrrow = double.Parse(principalBorrowTextBox.Text);
			years = double.Parse(yearsTextBox.Text);
			months = double.Parse(MothsTextBox.Text);
			yearlyIntrestRate = double.Parse(yearlyIntrestRateTextBox.Text);
			noOfMonths = years * 12 + months;
			monthlyIntrestRate = yearlyIntrestRate / 12.0;

			monthlyRepayment = principalBorrrow * monthlyIntrestRate * (Math.Pow((1.0 + monthlyIntrestRate), noOfMonths)) / ((Math.Pow((1.0 + monthlyIntrestRate), noOfMonths)) - 1);

			MonthlyIntrestRaateTextBox.Text = (yearlyIntrestRate / 12.0).ToString("N4") + "%";
			MonthlyRepaymentIntrestAmountTextBox.Text = monthlyRepayment.ToString("N");


		}
		private void ExitButton_Click(object sender, RoutedEventArgs e)
		{
			this.Frame.Navigate(typeof(Menu));

		}
	}
}