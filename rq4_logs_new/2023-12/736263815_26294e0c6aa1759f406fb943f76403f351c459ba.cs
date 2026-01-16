namespace PerformanceAuditing.Contracts
{
    public interface IScopeProcessingServices
    {
        public Task DoWorkAsync(CancellationToken stoppingToken);
    }
}