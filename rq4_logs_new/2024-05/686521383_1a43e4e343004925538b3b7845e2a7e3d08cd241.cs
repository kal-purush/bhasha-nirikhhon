using System;
using System.Collections.Generic;
using System.Data;

namespace EMailService.Modal.DashboardCalculation
{
    public class ExpensesModel
    {
        public decimal TotalPayableToEmployees { set; get; }
        public decimal TotalPFByEmployer { set; get; }
        public decimal TotalProfessionalTax { set; get; }
        public int ForYear { set; get; }
        public int ForMonth { set; get; }
    }

    public class GSTExpensesModel
    {
        public decimal Amount { set; get; }
        public DateTime PaidOn { set; get; }
    }

    public class ProfitExpenseModel
    {
        public decimal Amount { set; get; }
        public int Month { set; get; }
        public int Year { set; get; }
    }

    public class AdminDashboardResponse
    {
        public List<ProfitExpenseModel> expensesModel { set; get; }
        public List<ProfitExpenseModel> profitModel { set; get; }
        public DataTable projects { set; get; }
        public DataTable clients { set; get; }
        public DataTable newJoinees { set; get; }
        public DataTable leaves { set; get; }
    }
}