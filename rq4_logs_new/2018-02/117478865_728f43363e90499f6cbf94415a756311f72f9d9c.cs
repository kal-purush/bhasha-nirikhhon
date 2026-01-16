using System;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Text;
using Windows.UI.Xaml.Media;
using Windows.UI;

namespace CryptFolio
{
    class PortfolioList
    {
        internal TickerJSONResult result;
        internal double addedAmount;

        public PortfolioList()
        {

        }

        private FrameworkElement GetChildOfStackPanel(StackPanel parentPanel, string name)
        {
            foreach (FrameworkElement child in parentPanel.Children)
            {
                if (child.Name == name)
                {
                    return child;
                }
            }

            return null;
        }

        internal void DisplayCurrencyStats(string ticker, string displayName, ref StackPanel stackPanelRight)
        {
            TextBlock currencyLabel, usdLabel, btcLabel, userAmountLabel, userUSDValueLabel;
            CustomStackPanel stackPanelToUpdate;

            string
                stackPanelStr = ticker + "StackPanel",
                labelStr = ticker + "Label",
                labelContentStr = displayName + " Stats:",
                labelUSDStr = ticker + "USD",
                labelUSDContentStr = "USD Price: $" + result.price_usd,
                labelBTCStr = ticker + "BTC",
                labelBTCContentStr = "BTC Price: " + result.price_btc,
                labelUserAmountStr = ticker + "UserAmount",
                labelUserAmountContentStr = "Amount you own: ",
                labelUserUSDValueStr = ticker + "UserUSDVal",
                labelUserUSDValueContentStr = "Your estimated USD value: ";

            // add label if currency not already displayed
            stackPanelToUpdate = GetChildOfStackPanel(stackPanelRight, stackPanelStr) as CustomStackPanel;

            if (stackPanelToUpdate == null)
            {
                // create new StackPanel to hold labels
                stackPanelToUpdate = new CustomStackPanel
                {
                    Name = stackPanelStr,
                    Margin = new Thickness(5, 0, 5, 5)
                };

                // create and add currency label
                currencyLabel = new TextBlock
                {
                    FontWeight = FontWeights.Bold,
                    FontSize = 32,
                    Text = labelContentStr,
                    Name = labelStr
                };
                stackPanelToUpdate.Children.Add(currencyLabel);

                // create and add usdLabel
                usdLabel = new TextBlock
                {
                    Name = labelUSDStr,
                    FontSize = 22,
                    Text = labelUSDContentStr
                };
                stackPanelToUpdate.Children.Add(usdLabel);

                // create and add btcLabel
                btcLabel = new TextBlock
                {
                    Name = labelBTCStr,
                    FontSize = 22,
                    Text = labelBTCContentStr
                };
                stackPanelToUpdate.Children.Add(btcLabel);

                // create and add user holding amount label
                userAmountLabel = new TextBlock
                {
                    Name = labelUserAmountStr,
                    FontSize = 22,
                    Text = labelUserAmountContentStr + addedAmount.ToString()
                };
                stackPanelToUpdate.Children.Add(userAmountLabel);

                // create and add userUSDValue label
                userUSDValueLabel = new TextBlock
                {
                    Name = labelUserUSDValueStr,
                    FontSize = 22,
                    Text = UpdateUserUSDValueContentStr(ref stackPanelToUpdate, this.result.price_usd, labelUserUSDValueContentStr),
                    Foreground = new SolidColorBrush(Colors.ForestGreen)
                };
                stackPanelToUpdate.Children.Add(userUSDValueLabel);

                stackPanelRight.Children.Add(stackPanelToUpdate);
            }
            else
            {
                // update user value
                stackPanelToUpdate.UserValue = addedAmount * Convert.ToDouble(this.result.price_usd);

                // update price labels
                usdLabel = GetChildOfStackPanel(stackPanelToUpdate, labelUSDStr) as TextBlock;
                usdLabel.Text = labelUSDContentStr;
                btcLabel = GetChildOfStackPanel(stackPanelToUpdate, labelBTCStr) as TextBlock;
                btcLabel.Text = labelBTCContentStr;
                userUSDValueLabel = GetChildOfStackPanel(stackPanelToUpdate, labelUserUSDValueStr) as TextBlock;
                userUSDValueLabel.Text = UpdateUserUSDValueContentStr(ref stackPanelToUpdate, this.result.price_usd, labelUserUSDValueContentStr);
                userAmountLabel = GetChildOfStackPanel(stackPanelToUpdate, labelUserAmountStr) as TextBlock;
                userAmountLabel.Text = labelUserAmountContentStr + addedAmount.ToString();
            }

            // update total values
            //UpdateTotalValue();
        }

        internal double CalculateTotalInvestmentValue(ref StackPanel stackPanelRight)
        {
            double totalInvestmentValue = 0.0;
            foreach (CustomStackPanel child in stackPanelRight.Children)
            {
                totalInvestmentValue += child.UserValue;
            }

            return totalInvestmentValue;
        }

        private string UpdateUserUSDValueContentStr(ref CustomStackPanel sP, string priceStr, string contentStr)
        {
            // update user value
            sP.UserValue = addedAmount * Convert.ToDouble(priceStr);

            if (addedAmount == 0.0)
            {
                contentStr += "N/A";
            }
            else
            {
                contentStr += "$" + sP.UserValue.ToString();
            }

            return contentStr;
        }
    }
}