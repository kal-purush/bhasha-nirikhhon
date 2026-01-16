using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace BuxferToYNAB.Models
{

    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse);
    [DebuggerDisplay("memo:{memo}; amount:{amount}; date:{date}; cleared:{cleared}; cleared:{cleared}")]
    public class Transaction
    {
        public string id { get; set; }
        public DateTime date { get; set; }
        public decimal amount { get; set; }
        public string memo { get; set; }
        public string cleared { get; set; }
        public bool approved { get; set; }
        public string flag_color { get; set; }
        public string account_id { get; set; }
        public string account_name { get; set; }
        public string payee_id { get; set; }
        public string payee_name { get; set; }
        public string category_id { get; set; }
        public string category_name { get; set; }
        public object transfer_account_id { get; set; }
        public object transfer_transaction_id { get; set; }
        public object matched_transaction_id { get; set; }
        public string import_id { get; set; }
        public bool deleted { get; set; }
        public List<object> subtransactions { get; set; }
    }

    public class Data
    {
        public List<Transaction> transactions { get; set; }
        public int server_knowledge { get; set; }
    }

    public class TransactionWrapper
    {
        public Data data { get; set; }
    }


}