using Checkout.Payment.Api.Extensions;

namespace Checkout.Payment.Api.Tests;

public class Tests
{
    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public void MaskDigitsGivenValidString_MasksAllButLast4Characters()
    {
        var ccNumber = "1112343335";
        var maskedNumber = ccNumber.Mask();
        Assert.That(maskedNumber, Is.EqualTo("******3335"));
    }
}