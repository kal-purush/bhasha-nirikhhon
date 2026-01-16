using Appota.Data;
using Microsoft.EntityFrameworkCore;

namespace Appota.Schedule
{
    public class Scheduler
    {
        private readonly ApplicationDbContext _dbContext;
        private Timer _timer;

        public Scheduler(ApplicationDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public void Start()
        {
            _timer = new Timer(async (_) => await UpdateStatusIfNoChange(), null, TimeSpan.Zero, TimeSpan.FromMinutes(1));
        }

        private async Task UpdateStatusIfNoChange()
        {
            try
            {
                await _dbContext.Database.ExecuteSqlRawAsync("EXEC UpdateStatusIfNoChange");
            }
            catch (Exception ex)
            {
               
            }
        }
    }
}