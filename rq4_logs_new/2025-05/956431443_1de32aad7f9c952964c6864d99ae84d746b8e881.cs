namespace CSL.Tests;

using CSL;
using CSL.Exceptions;
using NUnit.Framework;
using Antlr4.Runtime;
using System;
using System.IO;

[TestFixture]
public class TestDuration
{
    [SetUp]
    public void Setup()
    {
    }

    [TestCase("10 min", 10 * Duration.MinuteFactor)]
    [TestCase("5 h", 5 * Duration.HourFactor)]
    [TestCase("2 d", 2 * Duration.DayFactor)]
    [TestCase("3 w", 3 * Duration.WeekFactor)]
    public void TestLiteralsMinuteParam(string input, int minutes)
    {
        var duration = DurationParser.ParseDuration(input);

        Assert.That(duration, Is.Not.Null);
        Assert.That(duration.Minutes, Is.EqualTo(minutes));
    }

    [TestCase("2 mth", 2 * Duration.MonthFactor)]
    [TestCase("1 y", 1 * Duration.YearFactor)]
    public void TestLiteralMonthParam(string input, int months)
    {
        var duration = DurationParser.ParseDuration(input);

        Assert.That(duration, Is.Not.Null);
        Assert.That(duration.Months, Is.EqualTo(months));
    }

    
    [TestCase("-3min")]       // Negative value
    [TestCase("tenmin")]      // Non-numeric value
    [TestCase("5minutes")]    // Incorrect unit abbreviation
    [TestCase("3/y")]         // Invalid separator
    [TestCase("min")]         // Missing number
    [TestCase("3")]           // Missing unit
    [TestCase("9999999999h")] // Excessively large value
    [TestCase("3.5 mth")]     // Decimal not allowed if parser expects integer
    public void TestInvalidLiteralsMinuteParam(string input)
    {
        Assert.Throws<InvalidLiteralCompilerException>(() => DurationParser.ParseDuration(input));
    }

}