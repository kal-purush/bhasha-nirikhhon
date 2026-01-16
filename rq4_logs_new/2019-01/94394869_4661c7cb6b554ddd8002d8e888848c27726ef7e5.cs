using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using YanSoft.CurrencyExchanger.WebApp.Utils;

namespace YanSoft.CurrencyExchanger.WebApp.Models
{
    public class CurrencyRatesHistoryResponse
    {
        public int Timestamp { get; set; }
        public string Base { get; set; }
        public string Target { get; set; }
        public int StartTimestamp { get; set; }
        public int EndTimestamp { get; set; }
        public List<CurrencyRateHistoryItem> Items { get; set; }

        public CurrencyRatesHistoryResponse()
        {
            Timestamp = DateTimeHelper.ConvertDateTimeToTimestamp(DateTime.UtcNow);
            Items = new List<CurrencyRateHistoryItem>();
        }
    }
}