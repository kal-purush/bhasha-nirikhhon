using System;
using System.Threading;
using NUnit.Framework;
using Nohros.Extensions;
using Nohros.Extensions.Time;

namespace Nohros.Metrics.Influx.Tests
{
  public class DatadogObserverTests
  {
    class ApiEndpointMock : IApiEndpoint
    {
      public bool PostSeries(string series) {
        PostedSeries = series;
        return true;
      }

      public string PostedSeries { get; set; }
    }

    [Test]
    public void should_serialize_measure_into_influx_format() {
      var tags =
        new Tags.Builder()
          .WithTag("tag1", "tagValue1")
          .Build();
      var config = new MetricConfig("myMetric", tags);
      var measure = new Measure(config, 1000);

      var date = DateTime.Now;
      var api = new ApiEndpointMock();
      var observer = new InfluxObserver(api, TimeSpan.FromMilliseconds(50));
      observer.Observe(measure, date);
      observer.Observe(measure, date);

      long epoch = date.ToUnixEpoch().ToNanos(TimeUnit.Seconds);
      string points = "myMetric,{0} value={1} {2}\nmyMetric,{0} value={1} {2}\n"
        .Fmt("tag1=tagValue1", measure.Value, epoch.ToString());

      Thread.Sleep(TimeSpan.FromMilliseconds(100));

      Assert.That(api.PostedSeries, Is.EqualTo(points));
    }
  }
}