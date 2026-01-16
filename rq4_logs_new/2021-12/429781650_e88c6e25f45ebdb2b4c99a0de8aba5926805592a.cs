using NUnit.Framework;
using System;
using Promotion.Engine.ConsoleApp;

namespace Promotion.Engine.ConsoleApp.UnitTests;
public class Tests1
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void TestAdd3PromotionRules()
    {   
        // Todo: Test that 3 promotion rules have been added by checking lenght or total price
        PromotionEngineViewModel promotionEngineViewModel = new PromotionEngineViewModel();
        promotionEngineViewModel.Input = "A,A,A";
        var expectedTotal = 130;
        var totalPrice = promotionEngineViewModel.TotalPrice;
        var result = expectedTotal == totalPrice;
        Assert.IsTrue(result, String.Format("Expected total price '{0}': true, but actual price '{1}': {2}", expectedTotal, totalPrice, result));
    }
}