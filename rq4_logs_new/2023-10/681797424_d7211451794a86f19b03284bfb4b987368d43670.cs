using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HOTELAPI1;
using HOTELAPI1.Services;

namespace HOTELAPI1
{
    public class BackGroundWorker : IHostedService, IDisposable
    {
        private readonly IServiceProvider _serviceProvider;
        private Timer _timer;

        public BackGroundWorker(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            // Ejecutar cada 5 minutos
            _timer = new Timer(UpdateReservacionStatus, null, TimeSpan.Zero, TimeSpan.FromMinutes(5));
            return Task.CompletedTask;
        }

        private void UpdateReservacionStatus(object state)
        {
            using (var scope = _serviceProvider.CreateScope())
            {
                var reservacionService = scope.ServiceProvider.GetRequiredService<ReservacionService>();
                reservacionService.UpdateReservacionStatusAsync().Wait();
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _timer?.Change(Timeout.Infinite, 0);
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _timer?.Dispose();
        }
    }
}
