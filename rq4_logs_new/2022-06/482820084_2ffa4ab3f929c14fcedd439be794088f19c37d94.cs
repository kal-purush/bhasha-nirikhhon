using ConsultationService.Services;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ConsultationService
{
    public class MongoHealthCheck : IHealthCheck
    {
        private IConsultationService _consultationService;

        public MongoHealthCheck(IConsultationService consultationService)
        {
            _consultationService = consultationService;
        }
        public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                var consultation = _consultationService.GetConsultation("");
                return Task.FromResult(HealthCheckResult.Healthy("Service is healthy"));
            }
            catch (Exception ex)
            {
                return Task.FromResult(new HealthCheckResult(context.Registration.FailureStatus, ex.Message));
            }            
        }
    }
}