using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Text;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Documents;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;

// The Blank Page item template is documented at https://go.microsoft.com/fwlink/?LinkId=234238

namespace Calculator
{
	public sealed partial class currencyConverter : Page
	{
		public currencyConverter()
		{
			this.InitializeComponent();
		}

		private void ConvertCurrency_Click(object sender, RoutedEventArgs e)
		{
			// Parse user input
			if (double.TryParse(AmountTextBox.Text, out double amount))
			{
				string fromCurrency = (FromCurrencyComboBox.SelectedItem as ComboBoxItem)?.Content.ToString();
				string toCurrency = (ToCurrencyComboBox.SelectedItem as ComboBoxItem)?.Content.ToString();

				// Perform currency conversion
				double conversionRate = GetConversionRate(fromCurrency, toCurrency);
				double resultAmount = amount * conversionRate;
				double inverseRate = 1 / conversionRate;

				// Generate output string
				string output = $"{amount} {fromCurrency} = \n" +
								$"<Run FontSize='24' FontWeight='Bold'>{resultAmount:F8}</Run> {toCurrency}\n\n" +
								$"1 {fromCurrency} = {conversionRate:F8} {toCurrency}\n" +
								$"1 {toCurrency} = {inverseRate:F8} {fromCurrency}";

				// Display output
				OutputTextBlock.Inlines.Clear();
				OutputTextBlock.Inlines.Add(new Run { Text = $"{amount} {fromCurrency} =\n\n" });
				OutputTextBlock.Inlines.Add(new Run { Text = $"{resultAmount:F8} {toCurrency}\n\n\n", FontSize = 24, FontWeight = FontWeights.Bold });
				OutputTextBlock.Inlines.Add(new Run { Text = $"\n1 {fromCurrency} = {conversionRate:F8} {toCurrency}\n\n" });
				OutputTextBlock.Inlines.Add(new Run { Text = $"1 {toCurrency} = {inverseRate:F8} {fromCurrency}" });
			}
			else
			{
				OutputTextBlock.Text = "Invalid amount entered.";
			}
		}


		// Function for conversion rates
		private double GetConversionRate(string fromCurrency, string toCurrency)
		{
			// From USD
			if (fromCurrency == "US Dollar" && toCurrency == "Euro") return 0.85189982;
			if (fromCurrency == "US Dollar" && toCurrency == "British Pound") return 0.72872436;
			if (fromCurrency == "US Dollar" && toCurrency == "Indian Rupee") return 74.257327;

			// From Euro
			if (fromCurrency == "Euro" && toCurrency == "US Dollar") return 1.1739732;
			if (fromCurrency == "Euro" && toCurrency == "British Pound") return 0.8556672;
			if (fromCurrency == "Euro" && toCurrency == "Indian Rupee") return 87.00755;

			// From British Pound
			if (fromCurrency == "British Pound" && toCurrency == "US Dollar") return 1.371907;
			if (fromCurrency == "British Pound" && toCurrency == "Euro") return 1.1686692;
			if (fromCurrency == "British Pound" && toCurrency == "Indian Rupee") return 101.68635;

			// From Indian Rupee
			if (fromCurrency == "Indian Rupee" && toCurrency == "US Dollar") return 0.011492628;
			if (fromCurrency == "Indian Rupee" && toCurrency == "Euro") return 0.013492774;
			if (fromCurrency == "Indian Rupee" && toCurrency == "British Pound") return 0.0098339397;

			// Default for same currency
			return 1.0;
		}

		private void Exit_Click(object sender, RoutedEventArgs e)
		{
			// Exit button to return to Main Menu
			Frame.Navigate(typeof(MainMenu));
		}
	}
}