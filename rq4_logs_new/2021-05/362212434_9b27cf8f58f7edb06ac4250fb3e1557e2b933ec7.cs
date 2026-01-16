using System;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using Service.Activity.Domain.Repositories;

namespace Service.Activity.Repositories
{
    public class ActivityRepository : IActivityRepository
    {
        private readonly IMongoDatabase _database;

        public ActivityRepository(IMongoDatabase database)
        {
            _database = database;
        }

        public async Task AddAsync(Domain.Models.Activity activity) =>
            await Collection
            .InsertOneAsync(activity);

        public async Task<Domain.Models.Activity> GetAsync(Guid id) =>
            await Collection
            .AsQueryable()
            .FirstOrDefaultAsync(activity => activity.Id == id);

        private IMongoCollection<Domain.Models.Activity> Collection => 
            _database.GetCollection<Domain.Models.Activity>("Activities");
    }
}