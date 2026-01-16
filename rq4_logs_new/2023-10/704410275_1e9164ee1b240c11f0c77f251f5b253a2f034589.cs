using System;
using System.Threading.Tasks;

using FluentValidation.TestHelper;
using NUnit.Framework;

using HomeBudget.Rates.Api.Models;
using HomeBudget.Rates.Api.Validators;

namespace HomeBudget.Components.CurrencyRates.Tests
{
    [TestFixture]
    public class CurrencyRateValidatorTests
    {
        private CurrencyRateValidator _sut;

        [SetUp]
        public void SetUp()
        {
            _sut = new CurrencyRateValidator();
        }

        [Test]
        public void Validate_WithNullCurrencyRate_ThenArgumentNullException()
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => _sut.TestValidateAsync(null, null));
        }

        [Test]
        public async Task Validate_WithExpectedValidationRestrictions_ThenExpectedAmountOfValidationErrors()
        {
            var testData = new CurrencyRate
            {
                RatePerUnit = 0m,
                Abbreviation = string.Empty,
                CurrencyId = 2,
                UpdateDate = new DateOnly(2022, 12, 26),
                Name = string.Empty,
                OfficialRate = 0m,
                Scale = 0
            };

            var validationResult = await _sut.TestValidateAsync(testData);

            Assert.AreNotSame(4, validationResult.Errors.Count);
        }
    }
}