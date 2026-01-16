using DeliveryApp.Core.Domain.CourierAggregate;
using Microsoft.EntityFrameworkCore;

namespace DeliveryApp.Infrastructure.Adapters.Postgres.Couriers;

public class CourierRepository(ApplicationDbContext dbContext) : ICourierRepository
{
    public Courier Add(Courier courier)
    {
        dbContext.Attach(courier.Transport);
        return dbContext.Couriers.Add(courier).Entity;
    }

    public void Update(Courier courier)
    {
        dbContext.Attach(courier.Transport);
        dbContext.Entry(courier).State = EntityState.Modified;
    }

    public async Task<Courier?> GetAsync(Guid courierId)
    {
        return await dbContext
                     .Couriers
                     .Include(x => x.Transport)
                     .FirstOrDefaultAsync(o => o.Id == courierId);
    }
}