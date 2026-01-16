using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WolfDen.Application.Requests.Commands.Attendence.Email;
using WolfDen.Domain.Entity;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Commands.Attendence.Service
{
    public class DailyAttendancePollerService : BackgroundService
    {
        private readonly ILogger<DailyAttendancePollerService> _logger;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        public DailyAttendancePollerService(IMediator mediator, ILogger<DailyAttendancePollerService> logger, IServiceScopeFactory serviceScopeFactory)
        {
            _logger = logger;
            _serviceScopeFactory = serviceScopeFactory;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                using (IServiceScope scope = _serviceScopeFactory.CreateScope())
                {
                    WolfDenContext _context = scope.ServiceProvider.GetRequiredService<WolfDenContext>();

                    _logger.LogInformation("Background service is running at: {time}", DateTimeOffset.Now);

                    List<DailyAttendence> newEntries = await _context.DailyAttendence
                    .Where(a => a.CreatedAt > DateTime.UtcNow.AddHours(24)).ToListAsync(stoppingToken);

                    foreach (DailyAttendence newEntry in newEntries)
                    {
                        if (newEntry.InsideDuration < 360)
                        {
                            IMediator _mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
                            SendEmailCommand sendEmailCommand = new SendEmailCommand();
                            sendEmailCommand.EmployeeId = newEntry.EmployeeId;
                            await _mediator.Send(sendEmailCommand, stoppingToken);
                            Console.WriteLine("send");
                        }
                    }
                    await Task.Delay(86400,stoppingToken);
                }
            }
        }
    }
}