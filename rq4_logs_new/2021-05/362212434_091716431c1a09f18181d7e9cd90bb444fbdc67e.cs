using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Library.Common.Mongo;
using MongoDB.Driver;
using Service.Activity.Domain.Models;
using Service.Activity.Domain.Repositories;

namespace Service.Activity.Services
{
    public class ActivitiesMongoSeeder : MongoSeeder
    {
        private readonly ICategoryRepository _categoryRepository;

        public ActivitiesMongoSeeder(IMongoDatabase database,
                                    ICategoryRepository categoryRepository) : base(database)
        {
            _categoryRepository = categoryRepository;
        }

        protected override async Task CustomSeedAsync()
        {
            var categories = new List<string>{
                "work",
                "sport",
                "hobby"
            };

            await Task.WhenAll(categories.Select(category => 
                _categoryRepository.AddAsync(new Category(category))));
        }
    }
}