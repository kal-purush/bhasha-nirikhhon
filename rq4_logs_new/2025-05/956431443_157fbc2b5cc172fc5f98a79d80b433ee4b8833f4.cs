namespace CSL.Tests.Literals;

using CSL;
using CSL.EventTypes;
using CSL.Exceptions;
using NUnit.Framework;
using Antlr4.Runtime;
using System;
using System.IO;

[TestFixture]

public class EventTests
{
    [SetUp]
    public void Setup()
    {
    }

    [TestCase(1, 1, 2024, 1 * Duration.DayFactor, (2024 * 12 + 1))]
    [TestCase(15, 6, 2023, 15 * Duration.DayFactor, (2023 * 12 + 6))]
    [TestCase(31, 12, 2025, 31 * Duration.DayFactor, (2025 * 12 + 12))]
    [TestCase(28, 2, 2024, 28 * Duration.DayFactor, (2024 * 12 + 2))]
    [TestCase(29, 2, 2024, 29 * Duration.DayFactor, (2024 * 12 + 2))]
    public void TestGetDateAsDuration(int day, int month, int year, int expectedMinutes, int expectedMonths)
    {
        var date = new Date(day, month, year);
        var duration = date.GetDateAsDuration();
        Assert.That(expectedMinutes, Is.EqualTo(duration.Minutes));
        Assert.That(expectedMonths, Is.EqualTo(duration.Months));
    }

    
    [TestCase(4, 4, 2024, 1, 1, 2024, true)]
    [TestCase(15, 6, 2024, 15, 5, 2024, true)]
    [TestCase(20, 5, 2024, 19, 5, 2024, true)]
    [TestCase(1, 1, 2025, 31, 12, 2024, true)]


    //Oskar fix den her 
    [TestCase(1, 2, 2024, 31, 1, 2024, true)]

    [TestCase(1, 1, 2024, 1, 1, 2024, true)]
    [TestCase(1, 5, 2023, 30, 4, 2023, true)]
    [TestCase(31, 12, 2023, 1, 1, 2024, false)]
    [TestCase(30, 4, 2024, 1, 5, 2024, false)]
    [TestCase(5, 5, 2024, 6, 5, 2024, false)]
    [TestCase(29, 2, 2024, 1, 3, 2024, false)]
    public void TestDateGreaterOrEqual(int firstDay, int firstMonth, int firstYear,
        int secondDay, int secondMonth, int secondYear, bool expectedResult)
    {
        var firstDate = new Date(firstDay, firstMonth, firstYear);
        var secondDate = new Date(secondDay, secondMonth, secondYear);
        var duration = secondDate.GetDateAsDuration();
        Assert.That(firstDate >= duration, Is.EqualTo(expectedResult));
    }
}