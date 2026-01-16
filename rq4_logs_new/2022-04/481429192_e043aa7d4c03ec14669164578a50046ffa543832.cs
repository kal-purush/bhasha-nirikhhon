using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace BuxferToYNAB.Models
{
    
    public class DateFormat
    {
        public string format { get; set; }
    }

    public class CurrencyFormat
    {
        public string iso_code { get; set; }
        public string example_format { get; set; }
        public int decimal_digits { get; set; }
        public string decimal_separator { get; set; }
        public bool symbol_first { get; set; }
        public string group_separator { get; set; }
        public string currency_symbol { get; set; }
        public bool display_symbol { get; set; }
    }

    [DebuggerDisplay("id:{id}; name:{name};")]
    public class Budget
    {
        public string id { get; set; }
        public string name { get; set; }
        public DateTime last_modified_on { get; set; }
        public string first_month { get; set; }
        public string last_month { get; set; }
        public DateFormat date_format { get; set; }
        public CurrencyFormat currency_format { get; set; }
    }

    public class Data
    {
        public List<Budget> budgets { get; set; }
        public object default_budget { get; set; }
    }

    public class BudgetWrapper
    {
        public Data data { get; set; }
    }

}