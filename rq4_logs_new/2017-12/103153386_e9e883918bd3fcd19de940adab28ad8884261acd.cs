using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Logic
{
    public class IkiCheckAnalyzer : ShopCheckAnalyzer
    {
        private List<string> _itemNames = new List<string>();
        private List<string> _prices = new List<string>();
        private List<string> _receiptList = new List<string>();
        private string _purchaseDate;
        private string _purchaseTime;


        /*patterns*/
        private string _pricePattern = @"^(\d+(?:[.,]\d{2})\s[A].?)$";
        private string _discountPattern = @"^(-\d+(?:[.,]\d{2})\s[A].?)$";
        private string _datePattern = @"[0-9]{4}-[0-1][0-9]-[0-3][0-9]";
        private string _timePattern = @"[0-9]{2}:[0-9]{2}:[0-9]{2}";
        private string _unitpricePattern = @"^(\d+(?:[.,]\d{3})\s[Kk][Gg]\s[Xx]\s\d+(?:[.,]\d{2})\s[Ee][Uu][Rr]/[Kk][Gg])$";



        public override void AnalyzeText(string receiptText)
        {

            _receiptList = TransformText(receiptText);
            ParseReceiptList(_receiptList);

            PayedPrices = _prices;
            ItemNames = _itemNames;
            PurchaseTime = Convert.ToDateTime((Regex.Replace(_purchaseDate, "[A-Za-z ]", String.Empty))
                + " " + (Regex.Replace(_purchaseTime, "[A-Za-z ]", String.Empty)));

            Prices = new List<string>(_prices);
        }

        private List<string> TransformText(string receiptText)
        {
            receiptText = receiptText.Substring(0, receiptText.IndexOf(@"\nSUMA\n"));
            var receiptList = new List<string>
                (receiptText.Split(
                    new string[] { "\\n" },
                    StringSplitOptions.RemoveEmptyEntries)
                    );

            receiptList = receiptList.Select(s => s.Replace(',', '.')).ToList();
            receiptList = receiptList.Select(s => s.Replace("\\", "")).ToList();

            return receiptList;
        }

        private void ParseReceiptList(List<string> receiptList)
        {
            for (int i = 0; i < receiptList.Count(); i++)
            {
                if (Regex.IsMatch(receiptList[i], _unitpricePattern))
                {
                    receiptList.RemoveAt(i);
                }

                if (Regex.IsMatch(receiptList[i], _datePattern))
                {
                    _purchaseDate = receiptList[i];
                    continue;
                }

                if (Regex.IsMatch(receiptList[i], _timePattern))
                {
                    _purchaseTime = receiptList[i];
                    continue;
                }

                if (Regex.IsMatch(receiptList[i], _discountPattern))
                {
                    receiptList[i] = receiptList[i].Replace(" A", String.Empty);

                    var temp = (Convert.ToDecimal(receiptList[i]) +
                        Convert.ToDecimal(_prices.Last())).ToString();

                    _prices.RemoveAt(_prices.Count() - 1);
                    _prices.Add(temp);
                    continue;
                }

                if (Regex.IsMatch(receiptList[i], _pricePattern))
                {
                    if (receiptList[i].Contains(" A."))
                    {
                        receiptList[i] = receiptList[i].Replace(" A.", String.Empty);
                    }
                    else receiptList[i] = receiptList[i].Replace(" A", String.Empty);

                    _prices.Add(receiptList[i]);
                    _itemNames.Add(receiptList[i - 1]);
                    continue;
                }
            }
        }
    }
}
