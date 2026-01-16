using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace BankingApp
{
    /// <summary>
    /// Interaction logic for Banking.xaml
    /// </summary>
    public partial class Banking : Window
    {
        decimal cSavingsBalance = 4346.37m;
        decimal cChequeBalance = 1386.37m;
        decimal cInvestmentBalance = 3138.78m;
        decimal amountTransfer = 0.0m;
        decimal fromBalance = 4346.37m, toBalance = 1386.37m;

        // use this two variables to keep track which radio button has clicked
        string fromRadioButtonChecked = "SavingsRadioButtonOn";
        String toRadioButtonChecked = "ChequeRadioButtonOn";
        public Banking()
        {
            InitializeComponent();
        }

        private void exitButton_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }


        private void fromChequeRadioButton_Checked(object sender, RoutedEventArgs e)
        {

        }

        private void fromInvestmentRadioButton_Checked(object sender, RoutedEventArgs e)
        {
            
        }


        private void transferButton_Click(object sender, RoutedEventArgs e)
        {
// working out the transfer amount

            // validate transfer amount input
            try
            {
                amountTransfer = decimal.Parse(amountTextBox.Text);
            }
            catch (FormatException)   // e.g. can trap error like 1000.50a
            {
                MessageBox.Show("Error! Please enter a valid amount");
                Keyboard.Focus(amountTextBox);
                amountTextBox.SelectAll();
            }
            catch (ArithmeticException)  // e.g. can trap error like a number divide by zero which may not happen in our case
            {
                MessageBox.Show("Error (Arithematic Expression)! Please enter a valid numeric amount");
                Keyboard.Focus(amountTextBox);
                amountTextBox.SelectAll();
            }
            catch (Exception)  // e.g. can trap general error  which may not happen in our case
            {
                MessageBox.Show("Error (General Exception)! Please enter a valid numeric amount");
                Keyboard.Focus(amountTextBox);
                amountTextBox.SelectAll();
            }

            if (amountTransfer < 0)  // error with negative transfer
            {
                MessageBox.Show("Error! You have negative amount. Enter another amount");
                Keyboard.Focus(amountTextBox);
                amountTextBox.SelectAll();
            }
            else
            // Handle insufficient fund
                if (amountTransfer > fromBalance)
                {
                    MessageBox.Show("Error! You have insufficient fund. Enter another amount");
                    Keyboard.Focus(amountTextBox);
                    amountTextBox.SelectAll();
                }
                else
                {
                    // Test which From radio button and which To radio button are on and calculate appropriate balance
                    string whichRadioButtonsAreChecked = fromRadioButtonChecked + "&" + toRadioButtonChecked;
                    switch (whichRadioButtonsAreChecked)
                    {

                        case "SavingsRadioButtonOn&SavingsRadioButtonOn":  // from Savings to Savings
                            MessageBox.Show("Error - cannot transfer from Savings to Savings");
                            break;

                        case "ChequeRadioButtonOn&ChequeRadioButtonOn":  // from Cheque to Cheque
                            MessageBox.Show("Error - cannot transfer from Cheque to Cheque");
                            break;

                        case "InvestmentRadioButtonOn&InvestmentRadioButtonOn":  // from Investment to Investment
                            MessageBox.Show("Error - cannot transfer from Investment to Investment");
                            break;


                        case "SavingsRadioButtonOn&ChequeRadioButtonOn":  // from Savings to Cheque
                            fromBalance = fromBalance - amountTransfer;
                            cSavingsBalance = fromBalance;
                            fromBalanceLabel.Content = cSavingsBalance.ToString("C");

                            toBalance = toBalance + amountTransfer;
                            cChequeBalance = toBalance;
                            toBalanceLabel.Content = cChequeBalance.ToString("C");
                            break;

                        case "SavingsRadioButtonOn&InvestmentRadioButtonOn":   // from Saving to Investment
                            fromBalance = fromBalance - amountTransfer;
                            cSavingsBalance = fromBalance;
                            fromBalanceLabel.Content = cSavingsBalance.ToString("C");

                            toBalance = toBalance + amountTransfer;
                            cInvestmentBalance = toBalance;
                            toBalanceLabel.Content = cInvestmentBalance.ToString("C");
                            break;

                        case "ChequeRadioButtonOn&SavingsRadioButtonOn":  // from Cheque to Savings
                            fromBalance = fromBalance - amountTransfer;
                            cChequeBalance = fromBalance;
                            fromBalanceLabel.Content = cChequeBalance.ToString("C");

                            toBalance = toBalance + amountTransfer;
                            cSavingsBalance = toBalance;
                            toBalanceLabel.Content = cSavingsBalance.ToString("C");
                            break;

                        case "ChequeRadioButtonOn&InvestmentRadioButtonOn":   // from Cheque to Investment
                            fromBalance = fromBalance - amountTransfer;
                            cChequeBalance = fromBalance;
                            fromBalanceLabel.Content = cChequeBalance.ToString("C");

                            toBalance = toBalance + amountTransfer;
                            cInvestmentBalance = toBalance;
                            toBalanceLabel.Content = cInvestmentBalance.ToString("C");
                            break;

                        case "InvestmentRadioButtonOn&SavingsRadioButtonOn":  // from Investment to Saving
                            fromBalance = fromBalance - amountTransfer;
                            cInvestmentBalance = fromBalance;
                            fromBalanceLabel.Content = cInvestmentBalance.ToString("C");

                            toBalance = toBalance + amountTransfer;
                            cSavingsBalance = toBalance;
                            toBalanceLabel.Content = cSavingsBalance.ToString("C");
                            break;

                        case "InvestmentRadioButtonOn&ChequeRadioButtonOn":  //from Investment to Cheque
                            fromBalance = fromBalance - amountTransfer;
                            cInvestmentBalance = fromBalance;
                            fromBalanceLabel.Content = cInvestmentBalance.ToString("C");

                            toBalance = toBalance + amountTransfer;
                            cChequeBalance = toBalance;
                            toBalanceLabel.Content = cChequeBalance.ToString("C");
                            break;
                    }
                }
        }

        private void toSavingsRadioButton_Click(object sender, RoutedEventArgs e)
        {
            toImage.Source = (ImageSource)Resources["savingsImage"];  // Change fromImage to ChequeAccount image
            toBalanceLabel.Content = cSavingsBalance.ToString("C");  // Change the from Balance to the Savings Account balance
            toBalance = cSavingsBalance;
            toRadioButtonChecked = "SavingsRadioButtonOn"; // this variable only stored a string
        }

        private void toChequeRadioButton_Click(object sender, RoutedEventArgs e)
        {
            toImage.Source = (ImageSource)Resources["chequeImage"];  // Change fromImage to ChequeAccount image
            toBalanceLabel.Content = cChequeBalance.ToString("C");  // Change the from Balance to the Savings Account balance
            toBalance = cChequeBalance;
            toRadioButtonChecked = "ChequeRadioButtonOn"; // this variable only stored a string
        }

        private void toInvestmentRadioButton_Click(object sender, RoutedEventArgs e)
        {
            toImage.Source = (ImageSource)Resources["investmentImage"];  // Change fromImage to ChequeAccount image
            toBalanceLabel.Content = cInvestmentBalance.ToString("C");  // Change the from Balance to the Savings Account balance
            toBalance = cInvestmentBalance;
            toRadioButtonChecked = "InvestmentRadioButtonOn"; // this variable only stored a string
        }

        private void fromSavingsRadioButton_Click(object sender, RoutedEventArgs e)
        {
            fromImage.Source = (ImageSource)Resources["savingsImage"];  // Change fromImage to ChequeAccount image
            fromBalanceLabel.Content = cSavingsBalance.ToString("C");  // Change the from Balance to the Savings Account balance
            fromBalance = cSavingsBalance;
            fromRadioButtonChecked = "SavingsRadioButtonOn";   // this variable only stored a string
        }

        private void fromChequeRadioButton_Click(object sender, RoutedEventArgs e)
        {
            fromImage.Source = (ImageSource)Resources["chequeImage"];  // Change fromImage to ChequeAccount image
            fromBalanceLabel.Content = cChequeBalance.ToString("C");  // Change the from Balance to the Savings Account balance
            fromBalance = cChequeBalance;
            fromRadioButtonChecked = "ChequeRadioButtonOn";  // this variable only stored a string
        }

        private void fromInvestmentRadioButton_Click(object sender, RoutedEventArgs e)
        {
            fromImage.Source = (ImageSource)Resources["investmentImage"];  // Change fromImage to ChequeAccount image
            fromBalanceLabel.Content = cInvestmentBalance.ToString("C");  // Change the from Balance to the Savings Account balance
            fromBalance = cInvestmentBalance;
            fromRadioButtonChecked = "InvestmentRadioButtonOn";  // this variable only stored a string
        }
    }
}