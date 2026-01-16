using Com.Danliris.Service.Sales.Lib.ViewModels.IntegrationViewModel;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Com.Danliris.Sales.Test.ViewModels.IntegrationViewModel
{
    public class AccountBankViewModelTest
    {
        [Fact]
        public void Should_Success_Instantiate()
        {
            int id = 1;
            string code = "code test";
            string AccountName = "AccountName test";
            string AccountNumber = "AccountNumber test";
            string BankName = "BankName test";
            string BankAddress = "BankAddress test";
            string AccountCurrencyId = "AccountCurrencyId test";
            string AccountCurrencyCode = "AccountCurrencyCode test";
            string SwiftCode = "SwiftCode test";
            CurrencyViewModel cvm = new CurrencyViewModel();

            AccountBankViewModel abvm = new AccountBankViewModel();
            abvm.Id = id;
            abvm.Code = code;
            abvm.AccountName = AccountName;
            abvm.AccountNumber = AccountNumber;
            abvm.BankName = BankName;
            abvm.BankAddress = BankAddress;
            abvm.AccountCurrencyId = AccountCurrencyId;
            abvm.AccountCurrencyCode = AccountCurrencyCode;
            abvm.SwiftCode = SwiftCode;
            abvm.Currency = cvm;

            Assert.Equal(id, abvm.Id);
            Assert.Equal(code, abvm.Code);
            Assert.Equal(AccountName, abvm.AccountName);
            Assert.Equal(AccountNumber, abvm.AccountNumber);
            Assert.Equal(BankName, abvm.BankName);
            Assert.Equal(BankAddress, abvm.BankAddress);
            Assert.Equal(AccountCurrencyId, abvm.AccountCurrencyId);
            Assert.Equal(AccountCurrencyCode, abvm.AccountCurrencyCode);
            Assert.Equal(SwiftCode, abvm.SwiftCode);
            Assert.Equal(cvm, abvm.Currency);

        }
    }
}