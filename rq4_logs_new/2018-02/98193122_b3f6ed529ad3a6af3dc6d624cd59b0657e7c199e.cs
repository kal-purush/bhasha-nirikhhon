using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PaderbornUniversity.SILab.Hip.UserStore
{
    /// <summary>
    /// A service that can be used with ASP.NET Core dependency injection.
    /// Usage: In ConfigureServices():
    /// <code>
    /// services.Configure&lt;UserStoreConfig&gt;(Configuration.GetSection("Endpoints"));
    /// services.AddSingleton&lt;UserStoreService&gt;();
    /// </code>
    /// </summary>
    public class DataStoreService
    {
        private readonly UserStoreConfig _config;
        private readonly IHttpContextAccessor _httpContextAccessor;

        public DataStoreService(IOptions<UserStoreConfig> config, ILogger<DataStoreService> logger,
            IHttpContextAccessor httpContextAccessor)
        {
            _config = config.Value;
            _httpContextAccessor = httpContextAccessor;

            if (string.IsNullOrWhiteSpace(config.Value.UserStoreHost))
                logger.LogWarning($"{nameof(UserStoreConfig.UserStoreHost)} is not configured correctly!");
        }

        public ActivityClient ExhibitPages => new ActivityClient(_config.UserStoreHost)
        {
            Authorization = _httpContextAccessor.HttpContext?.Request.Headers["Authorization"]
        };

        public PhotoClient Exhibits => new PhotoClient(_config.UserStoreHost)
        {
            Authorization = _httpContextAccessor.HttpContext?.Request.Headers["Authorization"]
        };

        public StudentDetailsClient History => new StudentDetailsClient(_config.UserStoreHost)
        {
            Authorization = _httpContextAccessor.HttpContext?.Request.Headers["Authorization"]
        };

        public UsersClient Media => new UsersClient(_config.UserStoreHost)
        {
            Authorization = _httpContextAccessor.HttpContext?.Request.Headers["Authorization"]
        };       
    }
}