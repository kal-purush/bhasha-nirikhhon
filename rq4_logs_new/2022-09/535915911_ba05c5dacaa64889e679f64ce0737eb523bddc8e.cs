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
	public sealed partial class CurrencyCalculator : Page
	{
		const double USD_TO_EUR = 0.85189982;
		const double USD_TO_GBP = 0.72872436;
		const double USD_TO_INR = 74.257327;

		const double EUR_TO_USD = 1.1739732;
		const double EUR_TO_GBP = 0.8556672;
		const double EUR_TO_INR = 87.00755;

		const double GBP_TO_USD = 1.371907;
		const double GBP_TO_EUR = 1.1686692;
		const double GBP_TO_INR = 101.68635;

		const double INR_TO_USD = 0.011492628;
		const double INR_TO_EUR = 0.013492774;
		const double INR_TO_GBP = 0.0098339397;

		const String TO_USD = " US Dollars";
		const String TO_EUR = " Euros";
		const String TO_GBP = " British Pounds";
		const String TO_INR = " Indian Rupees";

		public CurrencyCalculator()
		{
			this.InitializeComponent();
		}

		private void calculateButton_Click(object sender, RoutedEventArgs e)
		{
			double amount = double.Parse(amountTextBox.Text);
			double converted;
			String end;

			String from = fromComboBox.SelectedItem.ToString();
			String to = toComboBox.SelectedItem.ToString();

			switch (from)
			{
				case "USD - US Dollar":
					beforeCalculation.Text = amount.ToString() + " US Dollars =";
					switch (to)
					{
						case "EUR - Euro":
							converted = amount * USD_TO_EUR;
							afterCalculation.Text = converted.ToString() + TO_EUR;
							fromConversionTextBlock.Text = "1 USD = " + USD_TO_EUR + " Euros";
							toConversionTextBlock.Text = "1 EUR = " + EUR_TO_USD + " US Dollars";
                            break;
						case "GBP - British Pound":
							converted = amount * USD_TO_GBP;
							afterCalculation.Text = converted.ToString() + TO_GBP;
							fromConversionTextBlock.Text = "1 USD = " + USD_TO_GBP + " British Pounds";
							toConversionTextBlock.Text = "1 GBP = " + GBP_TO_USD + "US Dollars";
							break;
						case "INR - Indian Rupee":
							converted = amount * USD_TO_INR;
							afterCalculation.Text = converted.ToString() + TO_INR;
							fromConversionTextBlock.Text = "1 USD = " + USD_TO_INR + " Indian Rupees";
							toConversionTextBlock.Text = "1 INR = " + INR_TO_USD + "US Dollars";
							break;
						default:
							converted = amount;
							afterCalculation.Text = converted.ToString() + TO_USD;
							fromConversionTextBlock.Text = "1 USD = 1 US Dollar";
							toConversionTextBlock.Text = "1 USD = 1 US Dollar";
							break;
					}
					break;

				case "EUR - Euro":
					beforeCalculation.Text = amount.ToString() + " Euros =";
					switch (to)
					{
						case "USD - US Dollar":
							converted = amount * EUR_TO_USD;
							afterCalculation.Text = converted.ToString() + TO_USD;
							fromConversionTextBlock.Text = "1 EUR = " + EUR_TO_USD + " US Dollars";
							toConversionTextBlock.Text = "1 USD = " + USD_TO_EUR + " Euros";
							break;
						case "GBP - British Pound":
							converted = amount * EUR_TO_GBP;
							afterCalculation.Text = converted.ToString() + TO_GBP;
							fromConversionTextBlock.Text = "1 EUR = " + EUR_TO_GBP + " British Pounds";
							toConversionTextBlock.Text = "1 GBP = " + GBP_TO_EUR + " Euros";
							break;
						case "INR - Indian Rupee":
							converted = amount * EUR_TO_INR;
							afterCalculation.Text = converted.ToString() + TO_INR;
							fromConversionTextBlock.Text = "1 EUR = " + EUR_TO_INR + " Indian Rupees";
							toConversionTextBlock.Text = "1 INR = " + INR_TO_EUR + " Euros";
							break;
						default:
							converted = amount;
							afterCalculation.Text = converted.ToString() + TO_EUR;
							fromConversionTextBlock.Text = "1 EUR = 1 Euro";
							toConversionTextBlock.Text = "1 EUR = 1 Euro";
							break;
					}
					break;

				case "GBP - British Pound":
					beforeCalculation.Text = amount.ToString() + " British Pounds =";
					switch (to)
					{
						case "USD - US Dollar":
							converted = amount * GBP_TO_USD;
							afterCalculation.Text = converted.ToString() + TO_USD;
							fromConversionTextBlock.Text = "1 GBP = " + GBP_TO_USD + " US Dollars";
							toConversionTextBlock.Text = "1 USD = " + USD_TO_GBP + " British Pounds";
							break;
						case "EUR - Euro":
							converted = amount * GBP_TO_EUR;
							afterCalculation.Text = converted.ToString() + TO_EUR;
							fromConversionTextBlock.Text = "1 GBP = " + GBP_TO_EUR + " Euros";
							toConversionTextBlock.Text = "1 EUR = " + EUR_TO_GBP + " British Pounds";
							break;
						case "INR - Indian Rupee":
							converted = amount * GBP_TO_INR;
							afterCalculation.Text = converted.ToString() + TO_INR;
							fromConversionTextBlock.Text = "1 GBP = " + GBP_TO_INR + " Indian Rupees";
							toConversionTextBlock.Text = "1 INR = " + INR_TO_GBP + " British Pounds";
							break;
						default:
							converted = amount;
							afterCalculation.Text = converted.ToString() + TO_GBP;
							fromConversionTextBlock.Text = "1 GBP = 1 British Pound";
							toConversionTextBlock.Text = "1 GBP = 1 British Pound";
							break;
					}
					break;

				case "INR - Indian Rupee":
					beforeCalculation.Text = amount.ToString() + " Indian Rupees =";
					switch (to)
					{
						case "USD - US Dollar":
							converted = amount * INR_TO_USD;
							afterCalculation.Text = converted.ToString() + TO_USD;
							fromConversionTextBlock.Text = "1 INR = " + INR_TO_USD + " US Dollars";
							toConversionTextBlock.Text = "1 USD = " + USD_TO_INR + " Indian Rupees";
							break;
						case "EUR - Euro":
							converted = amount * INR_TO_EUR;
							afterCalculation.Text = converted.ToString() + TO_EUR;
							fromConversionTextBlock.Text = "1 INR = " + INR_TO_EUR + " Euros";
							toConversionTextBlock.Text = "1 EUR = " + EUR_TO_INR + " Indian Rupees";
							break;
						case "GBP - British Pound":
							converted = amount * INR_TO_GBP;
							afterCalculation.Text = converted.ToString() + TO_GBP;
							fromConversionTextBlock.Text = "1 INR = " + INR_TO_GBP + " British Pounds";
							toConversionTextBlock.Text = "1 GBP = " + GBP_TO_INR + " Indian Rupees";
							break;
						default:
							converted = amount;
							afterCalculation.Text = converted.ToString() + TO_INR;
							fromConversionTextBlock.Text = "1 INR = 1 Indian Rupee";
							toConversionTextBlock.Text = "1 INR = 1 Indian Rupee";
							break;
					}
					break;
			}


		}

		private void exitButton_Click(object sender, RoutedEventArgs e)
		{
			this.Frame.Navigate(typeof(Menu));

		}
	}
}