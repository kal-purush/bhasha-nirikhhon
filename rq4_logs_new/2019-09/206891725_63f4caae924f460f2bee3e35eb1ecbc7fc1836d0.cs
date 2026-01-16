using BudgetTracker.Business.Transactions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BudgetTracker.BudgetSquirrel.Application.Messages.TransactionApi
{
    public class FetchTransactionsArgumentApiMessage
    {
        [JsonProperty("budget-id")]
        public Guid BudgetId { get; set; }

        [JsonProperty("from-date")]
        public DateTime? FromDate { get; set; }

        [JsonProperty("to-date")]
        public DateTime? ToDate { get; set; }
    }
}