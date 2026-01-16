namespace CSL.Tests;

using CSL;
using CSL.Exceptions;
using NUnit.Framework;
using Antlr4.Runtime;
using System;
using System.IO;

[TestFixture]

public class DateTests
{
    [TestCase("6/5/2025", 6, 5, 2025)]      // Standard short date
    [TestCase("06/05/2025", 6, 5, 2025)]   // Leading zeroes
    [TestCase("29/02/2024", 29, 2, 2024)]  // Leap year
    [TestCase("31/12/1999", 31, 12, 1999)] // End of year
    [TestCase("1/1/1", 1, 1, 1)]           // Minimum valid date
    [TestCase("28/2/2100", 28, 2, 2100)]   // Valid non-leap century year
    [TestCase("30/04/2022", 30, 4, 2022)]  // 30-day month


    public void TestDateLiteral(string input, int days, int months, int years)
    {
        var date = DateParser.ParseDate(input);

        Assert.That(date, Is.Not.Null);
        Assert.That(date.Days, Is.EqualTo(days));
        Assert.That(date.Months, Is.EqualTo(months));
        Assert.That(date.Years, Is.EqualTo(years));
    }

    [TestCase("32/05/2025")] // Invalid day
    [TestCase("15/13/2025")] // Invalid month
    [TestCase("29/02/2023")] // Invalid non-leap year date
    [TestCase("00/01/2025")] // Zero day
    [TestCase("01/00/2025")] // Zero month
    [TestCase("1//2025")]    // Missing month
    [TestCase("/5/2025")]    // Missing day
    [TestCase("6/5/")]       // Missing year
    [TestCase("6-5-2025")]   // Wrong separator
    [TestCase("aa/bb/cccc")] // Non-numeric parts
    [TestCase("6/5")]        // Incomplete format
    [TestCase("")]           // Empty string
    [TestCase("6/5/10000")]  // Year out of range for DateTime
    public void TestInvalidDateLiterals(string input)
    {
        Assert.Throws<InvalidLiteralCompilerException>(() => DateParser.ParseDate(input));
    }
}