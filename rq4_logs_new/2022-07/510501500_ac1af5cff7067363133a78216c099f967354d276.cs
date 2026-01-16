using System;
using System.Threading.Tasks;
using FluentAssertions;
using Maybe.Exceptions;
using Maybe.Interfaces;
using NUnit.Framework;

namespace Maybe.Tests.TestCases;

public class JustTests : MaybeTestBase
{
    private const string DefaultValue = "Mercedes";
    private const string InvalidValue = "Volvo";
    private readonly string[] _invalidValues = { "Volkswagen", "BMW" };

    private const string ExceptionMessage = "This is an exception";

    private readonly Func<string, bool> _invalidityCheck = i => i == InvalidValue;

    [TestCase("Porsche")]
    [TestCase("McLaren")]
    public async Task MaybeWithJustAValidValue_BehavesCorrectly(string validValue)
    {
        // Act
        var sut = Maybe<string>.From(validValue);

        // Assert
        await TestJustValidity(sut, validValue, DefaultValue, ExceptionMessage);
    }

    [TestCase("Porsche")]
    [TestCase("McLaren")]
    public async Task MaybeWithInvalidityCheck_BehavesCorrectly(string validValue)
    {
        // Act
        var sut = Maybe<string>.From(validValue, _invalidityCheck);

        // Assert
        await TestJustValidity(sut, validValue, DefaultValue, ExceptionMessage);
    }

    [TestCase("Porsche")]
    [TestCase("McLaren")]
    public async Task MaybeWithInvalidityCheckAndInvalidValues_BehavesCorrectly(string validValue)
    {
        // Act
        var sut = Maybe<string>.From(validValue, _invalidityCheck, _invalidValues);

        // Assert
        await TestJustValidity(sut, validValue, DefaultValue, ExceptionMessage);
    }

    [TestCase("Porsche")]
    [TestCase("McLaren")]
    public async Task MaybeWithOnlyInvalidValues_BehavesCorrectly(string validValue)
    {
        // Act
        var sut = Maybe<string>.From(validValue, _invalidValues);

        // Assert
        await TestJustValidity(sut, validValue, DefaultValue, ExceptionMessage);
    }

    private async Task TestJustValidity(IMaybe<string> maybe, string validValue, string defaultValue,
        string exceptionMessage)
    {
        maybe.Should().BeOfType<Just<string>>();

        // Assert the basic checks
        maybe.GetOrElse(defaultValue).Should().Be(validValue);
        maybe.GetOrCall(() => defaultValue).Should().Be(validValue);

        // Assert the exception throwing methods
        maybe.Invoking(x => x.Get()).Should().NotThrow<MaybeValueNotFoundException>();
        maybe.Get().Should().Be(validValue);

        maybe.Invoking(x => x.GetOrThrow(new Exception(exceptionMessage))).Should().NotThrow<Exception>();
        maybe.GetOrThrow(new Exception(exceptionMessage)).Should().Be(validValue);

        // Assert the async stuff
        var asyncResult = await maybe.GetOrCallAsync(async () => await Task.FromResult(defaultValue));
        asyncResult.Should().Be(validValue);
    }
}