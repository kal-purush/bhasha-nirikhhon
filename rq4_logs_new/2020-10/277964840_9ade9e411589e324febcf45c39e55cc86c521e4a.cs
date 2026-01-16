using F1Telemetry.Core.Util.Export;
using FluentAssertions;
using System;
using Xunit;

namespace F1Telemetry.Test.ExporterTest
{
    public class ExporterTest
    {
        [Fact]
        public void Exporter_Should_Throw_InvalidOperationException_When_ExportEngine_Is_NotSpecified()
        {
            Action action = () =>
            {
                var exporter = new Exporter();

                exporter.ToFile("");
            };

            action.Should().Throw<InvalidOperationException>();
        }
    }
}