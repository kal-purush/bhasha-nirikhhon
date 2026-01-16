using UrlShortener.DataBase;
using UrlShortener.Models;
using UrlShortener.Services.Exceptions;
using UrlShortener.Services.UniqueStringGenerator;

namespace UrlShortener.Services.ShortUrlManager
{
    public class ShortUrlManager : IShortUrlManager
    {
        private readonly IDbContext _dbContext;
        private readonly IUniqueStringGenerator _uniqueStringGenerator;

        public ShortUrlManager(IDbContext dbContext, IUniqueStringGenerator uniqueStringGenerator)
        {
            _dbContext = dbContext;
            _uniqueStringGenerator = uniqueStringGenerator;
        }

        public async Task<UrlMappingEntry> AddUrlMappingEntryAsync(string originalUrl)
        {
            if (Uri.IsWellFormedUriString(originalUrl, UriKind.Absolute) == false)
            {
                throw new ArgumentException("Wrong url format");
            }

            var uniqueString = GenerateUniqueStringForCollection(_uniqueStringGenerator, _dbContext.UrlMappingEntries);

            var urlMappingEntry = new UrlMappingEntry()
            {
                ClicksCount = 0,
                Created = DateTime.Now,
                Url = originalUrl,
                Id = uniqueString,
            };

            _dbContext.UrlMappingEntries.Add(urlMappingEntry);

            await _dbContext.SaveChangesAsync();

            return urlMappingEntry;
        }

        public async Task DeleteUrlMappingEntryAsync(string id)
        {
            var entry = _dbContext.UrlMappingEntries.FirstOrDefault(x => x.Id == id);

            if (entry != null)
            {
                _dbContext.UrlMappingEntries.Remove(entry);

                await _dbContext.SaveChangesAsync();
            }
        }

        public IEnumerable<UrlMappingEntry> GetAllUrlMappingEntries()
        {
            return _dbContext.UrlMappingEntries;
        }

        public async Task<UrlMappingEntry> GetAndCountUrlMappingEntryAsync(string id)
        {
            var entry = _dbContext.UrlMappingEntries.FirstOrDefault(x => x.Id == id);

            if (entry == null)
                throw new NotFoundException($"Entry with id = {id} does not exist");

            entry.ClicksCount++;

            await _dbContext.SaveChangesAsync();

            return entry;
        }

        private static string GenerateUniqueStringForCollection(IUniqueStringGenerator uniqueStringGenerator,
            IQueryable<UrlMappingEntry> urlMappingEntries)
        {
            var uniqueString = string.Empty;

            UrlMappingEntry? entry = null;

            do
            {
                uniqueString = uniqueStringGenerator.Generate();
                entry = urlMappingEntries.FirstOrDefault(x => x.Id == uniqueString);
            }
            while (entry != null);

            return uniqueString;
        }
    }
}