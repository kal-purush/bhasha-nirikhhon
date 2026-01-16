using EasyTravel.Domain.Entites;
using EasyTravel.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasyTravel.Infrastructure.Services
{
    public class BookingCleanupBackgroundService : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;

        public BookingCleanupBackgroundService(IServiceScopeFactory scopeFactory)
        {
            _scopeFactory = scopeFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                using var scope = _scopeFactory.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

                var nowTime = DateTime.UtcNow.TimeOfDay;
                var expiredPhotographerBookingIds = await dbContext.PhotographerBookings
                    .Where(p => p.Booking != null &&
                                p.Booking.BookingStatus == BookingStatus.Pending &&
                                nowTime - p.StartTime >= TimeSpan.FromHours(3))
                    .Select(p => p.Id)
                    .ToListAsync();

                var expiredGuideBookingIds = await dbContext.GuideBookings
                    .Where(p => p.Booking != null &&
                                p.Booking.BookingStatus == BookingStatus.Pending &&
                                nowTime - p.StartTime >= TimeSpan.FromHours(3))
                    .Select(p => p.Id)
                    .ToListAsync();

                if (expiredPhotographerBookingIds.Any())
                {
                    var bookingsToDelete = await dbContext.Bookings
                        .Where(b => expiredPhotographerBookingIds.Contains(b.Id))
                        .ToListAsync();

                    dbContext.Bookings.RemoveRange(bookingsToDelete);
                    await dbContext.SaveChangesAsync();
                }
                if (expiredGuideBookingIds.Any())
                {
                    var bookingsToDelete = await dbContext.Bookings
                        .Where(b => expiredGuideBookingIds.Contains(b.Id))
                        .ToListAsync();

                    dbContext.Bookings.RemoveRange(bookingsToDelete);
                    await dbContext.SaveChangesAsync();
                }

                // Run every 1 minute
                await Task.Delay(TimeSpan.FromHours(1), stoppingToken);
            }
        }

    }

}