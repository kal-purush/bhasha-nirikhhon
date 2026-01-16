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

		private async void calculateButton_Click(object sender, RoutedEventArgs e)
		{
			double principal = 0;
			int years = 0;
			int months = 0;
			double yearlyInterest = 0;
			double monthlyInterest = 0;
			double monthlyRepayment = 0;

			try
			{
				principal = int.Parse(principalBorrowedTxtBox.Text);
			}
			catch(Exception theException)
			{
				var dialogMessage = new MessageDialog("The Principal must be a number and not blank " + theException.Message);
				await dialogMessage.ShowAsync();
				principalBorrowedTxtBox.Focus(FocusState.Programmatic);
				principalBorrowedTxtBox.SelectAll();
				return;
			}

			try
			{
				years = int.Parse(yearsTxtBox.Text);
			}
			catch(Exception theException)
			{
				var dialogMessage = new MessageDialog("years of the loan must be a number and not blank " + theException.Message);
				await dialogMessage.ShowAsync();
				yearsTxtBox.Focus(FocusState.Programmatic);
				yearsTxtBox.SelectAll();
				return;
			}

			try
			{
				months = int.Parse(monthsTxtBox.Text);
			}
			catch(Exception theException)
			{
				var dialogMessage = new MessageDialog("months of the loan must be a number and not blank " + theException.Message);
				await dialogMessage.ShowAsync();
				monthsTxtBox.Focus(FocusState.Programmatic);
				monthsTxtBox.SelectAll();
				return;
			}

			try
			{
				yearlyInterest = double.Parse(yearlyInterestTxtBox.Text);
			}
			catch(Exception theException)
			{
				var dialogMessage = new MessageDialog("The yearly interest must be a number and not blank " + theException.Message);
				await dialogMessage.ShowAsync();
				yearsTxtBox.Focus(FocusState.Programmatic);
				yearsTxtBox.SelectAll();
				return;
			}

			// calculating monthly interst rate
			monthlyInterest = (yearlyInterest / 100) / 12;
			monthlyInterestTxtBox.Text = monthlyInterest.ToString() + "%";

			// Calculating monthly repayments
			monthlyRepayment = principal * monthlyInterest / (1 - 1 / Math.Pow(1 + monthlyInterest, years * 12));
			monthlyRepaymentTxtBox.Text = monthlyRepayment.ToString("c");
		}

		private void exitButton_Click(object sender, RoutedEventArgs e)
		{
			this.Frame.Navigate(typeof(MainMenu));
		}
	}
}