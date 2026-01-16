using Com.Danliris.Service.Finance.Accounting.Lib.ViewModels.GarmentInvoicePurchasingDisposition;
using Com.Danliris.Service.Finance.Accounting.Lib.ViewModels.NewIntegrationViewModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Com.Danliris.Service.Finance.Accounting.Test.ViewModels
{
    public class VMValidationTest
    {
        [Fact]
        public void Should_Success_Validate_Garment_Invoice_Purchasing_Disposition1()
        {
            var vm = new GarmentInvoicePurchasingDispositionViewModel()
            {
                AccountBank = new AccountBankViewModel(),
                Supplier = new SupplierViewModel(),
                Items = new List<GarmentInvoicePurchasingDispositionItemViewModel>()
            };

            Assert.True(vm.Validate(null).Count() > 0);
        }

        [Fact]
        public void Should_Success_Validate_Garment_Invoice_Purchasing_Disposition2()
        {
            var vm = new GarmentInvoicePurchasingDispositionViewModel()
            {
                AccountBank = new AccountBankViewModel()
                {
                    Currency = new CurrencyViewModel()
                    {
                        Code = "IDR"
                    }
                },
                Supplier = new SupplierViewModel(),
                Items = new List<GarmentInvoicePurchasingDispositionItemViewModel>(),
                PaymentDate = DateTimeOffset.Now.AddMonths(1)
            };

            Assert.True(vm.Validate(null).Count() > 0);
        }

        [Fact]
        public void Shoul_Success_Build_Garment_Invoice_Disposition_Posting()
        {
            var vm = new GarmentInvoicePurchasingDispositionPostingViewModel()
            {
                ListIds = new List<GarmentInvoicePurchasingDispositionPostingIdViewModel>()
            };

            Assert.NotNull(vm.ListIds);

        }

        [Fact]
        public void Shoul_Success_Build_Garment_Invoice_Disposition_Posting_Id()
        {
            var vm = new GarmentInvoicePurchasingDispositionPostingIdViewModel();

            Assert.NotNull(vm);

        }

        [Fact]
        public void Should_Success_Validate_Garment_Invoice_Purchasing_Disposition_Item()
        {
            var vm = new GarmentInvoicePurchasingDispositionItemViewModel();

            Assert.True(vm.Validate(null).Count() > 0);
        }
    }
}