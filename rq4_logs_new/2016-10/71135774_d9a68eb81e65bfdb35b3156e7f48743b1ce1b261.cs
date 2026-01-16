using System.Collections.Generic;

namespace RestApiTests.Model
{
    public class Data
    {
        public string Design_Url { get; set; }
        public int Transactions_Total_Amount { get; set; }
        public int Tariff_Avg_Month_Balance { get; set; }
        public int Type { get; set; }
        public string Closing_Date { get; set; }
        public int Partial_Withdraw_Available { get; set; }
        public int Refill_Available { get; set; }
        public double Blocked_Amount { get; set; }
        public string Scheme_Id { get; set; }
        public string Pan { get; set; }
        public int Account_Id { get; set; }
        public string Title_Small { get; set; }
        public string Title { get; set; }
        public double Balance { get; set; }
        public string Currency { get; set; }
        public bool IsSalary { get; set; }
    }

    public class DataPattern
    {
        public string Design_Url { get; set; }
        public int Transactions_Total_Amount { get; set; }
        public int Tariff_Avg_Month_Balance { get; set; }
        public int Type { get; set; }
        public string Closing_Date { get; set; }
        public int Partial_Withdraw_Available { get; set; }
        public int Refill_Available { get; set; }
        public double Blocked_Amount { get; set; }
        public string Scheme_Id { get; set; }
        public string Pan { get; set; }
        public int Account_Id { get; set; }
        public string Title_Small { get; set; }
        public string Title { get; set; }
        public double Balance { get; set; }
        public string Currency { get; set; }
        public bool IsSalary { get; set; }

        public List<DataPattern> items { get; set; }

        public override string ToString()
        {
            return "{" + string.Format(
                $"\"design_url\":\"{Design_Url}\"," +
                $"\"transactions_total_amount\":{Transactions_Total_Amount}," +
                $"\"tariff_avg_month_balance\":{Tariff_Avg_Month_Balance}," +
                $"\"type\":{Type}," +
                $"\"closing_date\":\"{Closing_Date}\"," +
                $"\"partial_withdraw_available\":{Partial_Withdraw_Available}," +
                $"\"refill_available\":{Refill_Available}," +
                $"\"blocked_amount\":{Blocked_Amount}," +
                $"\"scheme_id\":\"{Scheme_Id}\"," +
                $"\"pan\":\"{Pan}\"," +
                $"\"account_id\":{Account_Id}," +
                $"\"title_small\":\"{Title_Small}\"," +
                $"\"title\":\"{Title}\"," +
                $"\"balance\":{Balance}," +
                $"\"currency\":\"{Currency}\"," +
                $"\"isSalary\":{IsSalary}"
                ) + "}";
        }
    }
}