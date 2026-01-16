using Com.Danliris.Sales.Test.BussinesLogic.Utils;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Facades.SalesReceipt;
using Com.Danliris.Service.Sales.Lib.Models.SalesReceipt;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Com.Danliris.Sales.Test.BussinesLogic.DataUtils.SalesReceipt
{
    public class SalesReceiptDataUtil : BaseDataUtil<SalesReceiptFacade, SalesReceiptModel>
    {
        public SalesReceiptDataUtil(SalesReceiptFacade facade) : base(facade)
        {
        }

        public override async Task<SalesReceiptModel> GetNewData()
        {
            return new SalesReceiptModel()
            {
                Code = "code",
                AutoIncreament = 1,
                SalesReceiptNo = "SalesReceiptNo",
                SalesReceiptType = "A",
                SalesReceiptDate = DateTimeOffset.UtcNow,
                BankId = 1,
                AccountCOA = "AccountCOA",
                AccountName = "AccountName",
                AccountNumber = "AccountNumber",
                BankName = "BankName",
                BankCode = "BankCode",
                BuyerId = 1,
                BuyerName = "BuyerName",
                BuyerAddress = "BuyerAddress",
                
                SalesReceiptDetails = new List<SalesReceiptDetailModel>()
                {
                    new SalesReceiptDetailModel()
                    {
                        SalesInvoiceId = 1,
                        SalesInvoiceNo = "SalesInvoiceNo",
                        DueDate = DateTimeOffset.UtcNow,
                        CurrencyId = 1,
                        CurrencyCode = "IDR",
                        CurrencySymbol = "Rp",
                        CurrencyRate = 14000,
                        UseVat = true,
                        TotalAmount = 1,
                        Paid = 1,
                        Nominal = 1,
                        Unpaid = 1
                    }
                }
            };
        }
    }
}