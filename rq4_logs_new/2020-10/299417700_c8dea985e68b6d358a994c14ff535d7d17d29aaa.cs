using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using WebAdvertisements.AdvertApi.Services;

namespace WebAdvertisements.AdvertApi.HealthChecks
{
    public class StorageHealthCheck : IHealthCheck
    {
        private readonly IAdvertisementStorageService _advertisementStorage;

        public StorageHealthCheck(IAdvertisementStorageService advertisementStorage)
        {
            _advertisementStorage = advertisementStorage;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context,
            CancellationToken cancellationToken = default)
        {
            var isStorageOk = await _advertisementStorage.CheckHealthAsync();
            return isStorageOk ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy();
        }
    }
}