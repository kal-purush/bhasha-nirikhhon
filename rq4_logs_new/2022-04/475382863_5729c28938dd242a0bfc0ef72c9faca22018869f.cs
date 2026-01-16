using System;
using CleanArchitecture_Example.Scripts.Domain;
using CleanArchitecture_Example.Scripts.Domain.UseCases;
using NUnit.Framework;
using NSubstitute;

namespace CleanArchitecture_Example.Tests.Editor
{
    public class PurchaseBundleUseCaseTest
    {
        [TestCase(1)]
        [TestCase(10)]
        [TestCase(999)]
        public void GivenServerResponseSucceed_WhenUserHasEnoughCurrency_PlayerInventoryIsUpdated(int bundleCost)
        {
            var bundlesRepository = Substitute.For<IShopBundlesRepository>();
            bundlesRepository.GetBundleById(Arg.Any<int>()).Returns(
                new ShopBundleDto(0, string.Empty, new Currency(string.Empty, bundleCost), Substitute.For<ICommodity>(),string.Empty)
            );
            var playerInventory = Substitute.For<IPlayerInventory>();
            var endpointHelper = Substitute.For<IEndpointHelper>();
            endpointHelper.PurchaseBundle(
                Arg.Any<int>(),
                Arg.Do<Action>(x => x.Invoke()),
                Arg.Any<Action>()
            );
            var useCase = new PurchaseBundleUseCase(bundlesRepository, endpointHelper, playerInventory);
            
            useCase.Purchase(0, null, null);
            
            playerInventory.Received(1).AddCurrency(Arg.Any<string>(), -bundleCost);
        }
    }
}