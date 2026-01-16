using System;
using System.Collections.Generic;

namespace BuxferToYNAB.Models
{
 
    public class TransactionDTO
    {
        public string account_id { get; set; }
        public DateTime date { get; set; }
        public int amount { get; set; }
        public string payee_name { get; set; }
        public string memo { get; set; }
        public string flag_color { get; set; }
        public string import_id { get; set; }
    }

    public class TransactionsDTO
    {
        public TransactionsDTO()
        {
            transactions = new List<TransactionDTO>();
        }

        public List<TransactionDTO> transactions { get; set; }
    }

}