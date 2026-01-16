using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.ServiceModel.Channels;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.Globalization.NumberFormatting;
using Windows.UI.Popups;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;

namespace Calculator
{
	public sealed partial class CurrencyConverter : Page
	{
		// Constructor to initialize the CurrencyConverter page - AK
		public CurrencyConverter()
		{
			this.InitializeComponent();
		}


		// Event handler for the conversion button
		private void btnConvertCurrency_Click(object sender, RoutedEventArgs e)
		{
			// Get the amount entered by the user - AK
			double amount = 0;
			if (!double.TryParse(AmountInput.Text, out amount))
			{
				ConversionResult.Text = "Please enter a valid amount.";
				return;
			}

			// Get selected currencies from combo boxes - AK
			string fromCurrency = (FromCurrency.SelectedItem as ComboBoxItem)?.Content.ToString();
			string toCurrency = (ToCurrency.SelectedItem as ComboBoxItem)?.Content.ToString();

			// Conversion rates for testing (hard-coded for simplicity) - AK
			double conversionRate = GetConversionRate(fromCurrency, toCurrency);

			if (conversionRate == 0)
			{
				ConversionResult.Text = "Conversion rate not found.";
				return;
			}

			// Perform the currency conversion - AK
			double convertedAmount = amount * conversionRate;

			// Display the result
			ConversionResult.Text = $"{amount} {fromCurrency.Split('-')[1].Trim()} = {convertedAmount} {toCurrency.Split('-')[1].Trim()}";
		}

		// Event handler for the exit button - AK
		private void Exit_Click(object sender, RoutedEventArgs e)
		{
			// Navigate back to the Navigation Page - AK
			Frame.Navigate(typeof(NavigationPage));
		}

		// Helper method to get the conversion rate based on selected currencies - AK
		private double GetConversionRate(string fromCurrency, string toCurrency)
		{
			// Hardcoded conversion rates for testing - AK
			if (fromCurrency == "USD - US Dollar" && toCurrency == "EUR - Euro") return 0.85189982;
			if (fromCurrency == "USD - US Dollar" && toCurrency == "GBP - British Pound") return 0.72872436;
			if (fromCurrency == "USD - US Dollar" && toCurrency == "INR - Indian Rupee") return 74.257327;

			if (fromCurrency == "EUR - Euro" && toCurrency == "USD - US Dollar") return 1.1739732;
			if (fromCurrency == "EUR - Euro" && toCurrency == "GBP - British Pound") return 0.8556672;
			if (fromCurrency == "EUR - Euro" && toCurrency == "INR - Indian Rupee") return 87.00755;

			if (fromCurrency == "GBP - British Pound" && toCurrency == "USD - US Dollar") return 1.371907;
			if (fromCurrency == "GBP - British Pound" && toCurrency == "EUR - Euro") return 1.1686692;
			if (fromCurrency == "GBP - British Pound" && toCurrency == "INR - Indian Rupee") return 101.68635;

			if (fromCurrency == "INR - Indian Rupee" && toCurrency == "USD - US Dollar") return 0.011492628;
			if (fromCurrency == "INR - Indian Rupee" && toCurrency == "EUR - Euro") return 0.013492774;
			if (fromCurrency == "INR - Indian Rupee" && toCurrency == "GBP - British Pound") return 0.0098339397;

			// Return 0 if no valid conversion rate is found - AK
			return 0;
		}
	}
}