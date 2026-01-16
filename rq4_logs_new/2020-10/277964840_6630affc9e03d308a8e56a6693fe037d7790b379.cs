using F1Telemetry.Core;
using F1Telemetry.Core.Data;
using FluentAssertions;
using System;
using System.Collections.Generic;
using Xunit;

namespace F1Telemetry.Test.Core
{
    public class LapSummaryTest
    {
        [Theory]
        [InlineData(100.12f)]
        [InlineData(2000.0f)]
        [InlineData(30000.0f)]
        [InlineData(1234.45f)]
        public void Given_ERSDeployed_Percentage_Should_Return_Based_On_TheMax_DeployableERS(float ersDeployed)
        {
            var expected = (float)Math.Round(ersDeployed / CarInfo.F1.MaxDeployableERS, 2);

            var carStatusData = new List<CarStatusData>
            {
                new CarStatusData(0, 0)
                {
                    ErsDeployedThisLap = ersDeployed
                }
            };

            var lapSummary = new LapSummary(0, 0, new List<LapData>(), new List<CarStatusData>(), new List<CarTelemetryData>());

            var actual = lapSummary.ERSDeployedPercentage;

            actual.Should().BeApproximately(expected, 0.05f);
        }
    }
}