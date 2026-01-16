using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Threading.Tasks;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Core;
using Windows.UI.Popups;
using Windows.UI.ViewManagement;
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
	public sealed partial class CurrencyConverter : Page
	{
		public CurrencyConverter()
		{
			this.InitializeComponent();

		}

		//To fix the page size
		private void FormName_SizeChanged(object sender, SizeChangedEventArgs e)
		{

			ApplicationView.GetForCurrentView().TryResizeView(new Size(900, 600));
		}

		//constants
		const double USDOLLAR_TO_EURO = 0.85189982;
		const double USDOLLAR_TO_BPOUND = 0.72872436;
		const double USDOLLAR_TO_INDRUPEE = 74.257327;
		const double EURO_TO_USDOLLAR = 1.1739732;
		const double EURO_TO_BPOUND = 0.8556672;
		const double EURO_TO_INRUPEE = 87.00755;
		const double BPOUND_TO_USDOLLAR = 1.371907;
		const double BPOUND_TO_EURO = 1.1686692;
		const double BPOUND_TO_INRUPEE = 101.68635;
		const double INRUPEE_TO_USDOLLAR = 0.011492628;
		const double INRUPEE_TO_EURO = 0.013492774;
		const double INRUPEE_TO_BPOUND = 0.0098339397;

		//Declaration of class level variables
		private double currencyAmount = 0;
		private double convertedAmount = 0;
		private int selectedSource = 0;
		private int selectedTarget = 0;
		private string currencyIn = "";
		private string currencyOut = "";
		private double sourceRate = 0;
		private double targetRate = 0;

		private async void btnCurrencyConversion_Click(object sender, RoutedEventArgs e)
		{
			//validate the amount entered
			try
			{
				//contents of txtAmount is converted from string to double
				currencyAmount = double.Parse(txtAmount.Text);

			}
			catch (Exception theException)
			{
				//this is executed if any formatting issues arises within try block
				//system generates an error message with the exception
				//then displays the message to the screen
				//then it set focus to the txtAmount field
				//selects the contents within txtAmount
				//returns to the code
				MessageDialog dialogMessage = new MessageDialog("Error! Please enter a valid currency amount " + theException.Message);
				_ = await dialogMessage.ShowAsync();
				txtAmount.Focus(FocusState.Programmatic);
				txtAmount.SelectAll();
				return;
			}

			//validating currency amount for greater than zero
			if (currencyAmount <= 0)
			{
				var dialogMessage = new MessageDialog("Error! Currency amount must be greater than zero. ");
				await dialogMessage.ShowAsync();
				txtAmount.Focus(FocusState.Programmatic);
				txtAmount.SelectAll();
				return;
			}

			//calculations
			selectedSource = cmbFrom.SelectedIndex;
			selectedTarget = cmbTo.SelectedIndex;

			switch (selectedSource)
			{
				case 0:
					//If usdollar selected
					currencyIn = "US Dollars";
					if (selectedTarget == 0)
					{
						//usdollar to usdollar
						currencyOut = "US Dollars";
						sourceRate = (double)1.0m;
						targetRate = (double)1.0m;
						convertedAmount = currencyAmount;
					}
					else if (selectedTarget == 1)
					{
						//usdollar to euro
						currencyOut = "Euros";
						sourceRate = USDOLLAR_TO_EURO;
						targetRate = EURO_TO_USDOLLAR;
						convertedAmount = currencyAmount * USDOLLAR_TO_EURO;
					}
					else if (selectedTarget == 2)
					{
						//usdollar to bpound
						currencyOut = "British Pounds";
						sourceRate = USDOLLAR_TO_BPOUND;
						targetRate = BPOUND_TO_USDOLLAR;
						convertedAmount = currencyAmount * USDOLLAR_TO_BPOUND;
					}
					else
					{
						//usdollar to inrupee
						currencyOut = "Indian Rupees";
						sourceRate = USDOLLAR_TO_INDRUPEE;
						targetRate = INRUPEE_TO_USDOLLAR;
						convertedAmount = currencyAmount * USDOLLAR_TO_INDRUPEE;
					}
					break;
				case 1:
					//if euro selected
					currencyIn = "Euros";
					if (selectedTarget == 0)
					{
						//euro to usdollar
						currencyOut = "US Dollars";
						sourceRate = EURO_TO_USDOLLAR;
						targetRate = USDOLLAR_TO_EURO;
						convertedAmount = currencyAmount * EURO_TO_USDOLLAR;
					}
					else if (selectedTarget == 1)
					{
						//euro to euro
						currencyOut = "Euros";
						sourceRate = (double)1.0m;
						targetRate = (double)1.0m;
						convertedAmount = currencyAmount;
					}
					else if (selectedTarget == 2)
					{
						//euro to bpound
						currencyOut = "British Pounds";
						sourceRate = EURO_TO_BPOUND;
						targetRate = BPOUND_TO_EURO;
						convertedAmount = currencyAmount * EURO_TO_BPOUND;
					}
					else
					{
						//euro to inrupee
						currencyOut = "Indian Rupees";
						sourceRate = EURO_TO_INRUPEE;
						targetRate = INRUPEE_TO_EURO;
						convertedAmount = currencyAmount * EURO_TO_INRUPEE;
					}
					break;
				case 2:
					//if british pound selected
					currencyIn = "British Pounds";
					if (selectedTarget == 0)
					{
						//bpound to usdollar
						currencyOut = "US Dollars";
						sourceRate = BPOUND_TO_USDOLLAR;
						targetRate = USDOLLAR_TO_BPOUND;
						convertedAmount = currencyAmount * BPOUND_TO_USDOLLAR;
					}
					else if (selectedTarget == 1)
					{
						//bpound to euro
						currencyOut = "Euros";
						sourceRate = BPOUND_TO_EURO;
						targetRate = EURO_TO_BPOUND;
						convertedAmount = currencyAmount * BPOUND_TO_EURO;
					}
					else if (selectedTarget == 2)
					{
						//bpound to bpound
						currencyOut = "British Pounds";
						sourceRate = (double)1.0m;
						targetRate = (double)1.0m;
						convertedAmount = currencyAmount;
					}
					else
					{
						//bpound to inrupee
						currencyOut = "Indian Rupees";
						sourceRate = BPOUND_TO_INRUPEE;
						targetRate = INRUPEE_TO_BPOUND;
						convertedAmount = currencyAmount * BPOUND_TO_INRUPEE;
					}
					break;
				case 3:
					//if indian rupee selected
					currencyIn = "Indian Rupees";
					if (selectedTarget == 0)
					{
						//inrupee to usdollar
						currencyOut = "US Dollars";
						sourceRate = INRUPEE_TO_USDOLLAR;
						targetRate = INRUPEE_TO_BPOUND;
						convertedAmount = currencyAmount * INRUPEE_TO_USDOLLAR;
					}
					else if (selectedTarget == 1)
					{
						//inrupee to euro
						currencyOut = "Euros";
						sourceRate = INRUPEE_TO_EURO;
						targetRate = EURO_TO_INRUPEE;
						convertedAmount = currencyAmount * INRUPEE_TO_EURO;
					}
					else if (selectedTarget == 2)
					{
						//inrupee to bpound
						currencyOut = "British Pounds";
						sourceRate = INRUPEE_TO_BPOUND;
						targetRate = BPOUND_TO_INRUPEE;
						convertedAmount = currencyAmount * INRUPEE_TO_BPOUND;
					}
					else
					{
						//inrupee to inrupee
						currencyOut = "Indian Rupees";
						sourceRate = (double)1.0m;
						targetRate = (double)1.0m;
						convertedAmount = currencyAmount;
					}
					break;
			}

			displayInitialMessage(currencyIn);
			displayResults(convertedAmount);
			displaySourceCurrency(currencyIn);
			displayTargetCurrency(currencyOut);


		}

		private void btnExit_Click(object sender, RoutedEventArgs e)
		{
			this.Frame.Navigate(typeof(MainMenu));
		}

		private double convertCurrency(string sourceCurrency, string targetCurrency)
		{

			return 10;
		}

		public void displayInitialMessage(string currIn)
		{
			//displays the amount entered with its currency type when source currency type passed in
			lblDisplayAmount.Text = currencyAmount.ToString() + " " + currIn + " = ";
		}

		public void displayResults(double finalAmount)
		{
			//displays currency coversion results
			lblConvertedAmount.Text = finalAmount.ToString("F8") + " " + currencyOut;
		}

		public void displaySourceCurrency(string sourceCurrency)
		{
			//display the from currency rate
			lblFromCurrency.Text = "1 " + currencyIn + " = " + sourceRate.ToString() + " " + currencyOut;
		}

		public void displayTargetCurrency(string targetCurrency)
		{
			//display the to currency rate
			lblToCurrency.Text = "1 " + currencyOut + " = " + targetRate.ToString() + " " + currencyIn;
		}


	}
}