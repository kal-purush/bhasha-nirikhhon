using Com.Danliris.Service.Sales.Lib.ViewModels.FinishingPrinting;
using Com.Danliris.Service.Sales.Lib.ViewModels.IntegrationViewModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Com.Danliris.Sales.Test.ViewModels.FinishingPrinting
{
  public  class FinishingPrintingSalesContractViewModelTest
    {
        [Fact]
        public void Should_Success_Instantiate_FinishingPrintingSalesContractViewModel()
        {
            FinishingPrintingSalesContractViewModel viewModel = new FinishingPrintingSalesContractViewModel();

        }

        [Fact]
        public void Validate_Default()
        {
            FinishingPrintingSalesContractViewModel viewModel = new FinishingPrintingSalesContractViewModel();
            viewModel.OrderQuantity = -1;

            var defaultValidationResult = viewModel.Validate(null);
            Assert.True(defaultValidationResult.Count() > 0);
        }


        [Fact]
        public void Validate_With_BuyerType_Export()
        {
            FinishingPrintingSalesContractViewModel viewModel = new FinishingPrintingSalesContractViewModel();
            viewModel.Amount = 0;
            viewModel.Agent = new AgentViewModel()
            {
                Id = 1
            };
            viewModel.Commission = "";
            viewModel.PointSystem = 4;
            viewModel.PointLimit = 0;
            viewModel.Details = new List<FinishingPrintingSalesContractDetailViewModel>()
            {
                new FinishingPrintingSalesContractDetailViewModel()
                {
                    Id=1,
                    Color ="",
                    Price =0,
                },
                new FinishingPrintingSalesContractDetailViewModel()
                {
                    Id=1,
                    Color ="",
                    Price =0,
                },
            };
            BuyerViewModel buyer = new BuyerViewModel()
            {
                Id = 0,
                Name = "Name",
                Type = "ekspor",
                
            };
            viewModel.Buyer = buyer;
            viewModel.OrderQuantity = 1;

            var defaultValidationResult = viewModel.Validate(null);
            Assert.True(defaultValidationResult.Count() > 0);
        }

    }
}