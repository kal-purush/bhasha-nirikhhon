using Altinn.Auth.AuditLog.Core.Models;
using Altinn.Auth.AuditLog.Services;
using Microsoft.Extensions.Time.Testing;
using System;
using System.Collections.Generic;
using Xunit;

namespace Altinn.Auth.AuditLog.Tests;
public class PartitionCreationHostedServiceTests
{
    [Fact]
    public void GetPartitionsForCurrentAndAdjacentMonths_ReturnsCorrectPartitions()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(new DateTime(2023, 10, 15));
        var service = new PartitionCreationHostedService(null, null, timeProvider);

        // Act
        var partitions = service.GetPartitionsForCurrentAndAdjacentMonths();

        // Assert
        Assert.Equal(6, partitions.Count);
        Assert.Contains(partitions, p => p.Name == "eventlogv1_y2023m09" && p.StartDate == new DateOnly(2023, 9, 1) && p.EndDate == new DateOnly(2023, 9, 30));
        Assert.Contains(partitions, p => p.Name == "eventlogv1_y2023m10" && p.StartDate == new DateOnly(2023, 10, 1) && p.EndDate == new DateOnly(2023, 10, 31));
        Assert.Contains(partitions, p => p.Name == "eventlogv1_y2023m11" && p.StartDate == new DateOnly(2023, 11, 1) && p.EndDate == new DateOnly(2023, 11, 30));
    }

    [Theory]
    [InlineData(2023, 10, 15, 2023, 10, 1, 2023, 10, 31)]
    [InlineData(2023, 2, 1, 2023, 2, 1, 2023, 2, 28)]
    [InlineData(2024, 2, 1, 2024, 2, 1, 2024, 2, 29)] // Leap year
    public void GetMonthStartAndEndDate_ReturnsCorrectDates(int year, int month, int day, int expectedStartYear, int expectedStartMonth, int expectedStartDay, int expectedEndYear, int expectedEndMonth, int expectedEndDay)
    {
        // Arrange
        var service = new PartitionCreationHostedService(null, null, null);
        var date = new DateOnly(year, month, day);

        // Act
        var (startDate, endDate) = service.GetMonthStartAndEndDate(date);

        // Assert
        Assert.Equal(new DateOnly(expectedStartYear, expectedStartMonth, expectedStartDay), startDate);
        Assert.Equal(new DateOnly(expectedEndYear, expectedEndMonth, expectedEndDay), endDate);
    }

    [Fact]
    public void GetPartitionsForCurrentAndAdjacentMonths_CrossYearBoundary_ReturnsCorrectPartitions()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(new DateTime(2023, 12, 15));
        var service = new PartitionCreationHostedService(null, null, timeProvider);

        // Act
        var partitions = service.GetPartitionsForCurrentAndAdjacentMonths();

        // Assert
        Assert.Equal(6, partitions.Count);
        Assert.Contains(partitions, p => p.Name == "eventlogv1_y2023m11" && p.StartDate == new DateOnly(2023, 11, 1) && p.EndDate == new DateOnly(2023, 11, 30));
        Assert.Contains(partitions, p => p.Name == "eventlogv1_y2023m12" && p.StartDate == new DateOnly(2023, 12, 1) && p.EndDate == new DateOnly(2023, 12, 31));
        Assert.Contains(partitions, p => p.Name == "eventlogv1_y2024m01" && p.StartDate == new DateOnly(2024, 1, 1) && p.EndDate == new DateOnly(2024, 1, 31));
    }
}