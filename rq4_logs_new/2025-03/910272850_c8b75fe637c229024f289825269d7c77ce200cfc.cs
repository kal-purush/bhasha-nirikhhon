using CsvHelper.Configuration.Attributes;
using Expenses.Api.PresentationContracts.PaymentMethods;
using Microsoft.Extensions.Localization;


namespace Expenses.Api.PresentationContracts.Expenses;

public class ExpenseFileResponse
{
    public DateTime TransactionDate { get; set; }
    public string Description { get; set; }
    public decimal TotalPrice { get; set; }
    public string Location { get; set; }
    public List<string> ExpenseCategories { get; set; }
    public PaymentMethodFileResponse PaymentMethod { get; set; }
    [Optional]
    public int? Installment { get; set; }
}