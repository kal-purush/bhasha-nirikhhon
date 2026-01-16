using Metrics;
using Metrics.Json;

namespace ServiceControl.Monitoring
{
    public interface IDerivedMetric
    {
        void Initialize(MetricsContext rootContext, MetricsContext receivedMetricContext);
        void Consume(JsonMetricsContext metricsData);
    }
}