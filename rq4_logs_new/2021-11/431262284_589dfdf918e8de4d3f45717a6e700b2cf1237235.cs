using System.Text.Json;
using TftTracker.Data.Entities;

namespace TftTracker.Data
{
    public class TftSeeder
    {
        private readonly TftContext _context;
        private readonly IWebHostEnvironment _environment;

        public TftSeeder(TftContext context, IWebHostEnvironment environment)
        {
            _context = context;
            _environment = environment;
        }

        public void Seed()
        {
            //Make sure DB exists
            _context.Database.EnsureCreated();

            if (!_context.Summoners.Any())
            {
                //Create sample data
                var filePath = Path.Combine(_environment.ContentRootPath, "Data/summoners.json");
                var json = File.ReadAllText(filePath);
                var summoners = JsonSerializer.Deserialize<IEnumerable<Summoner>>(json);

                _context.Summoners.AddRange(summoners);
                _context.SaveChanges();
            }
        }
    }
}