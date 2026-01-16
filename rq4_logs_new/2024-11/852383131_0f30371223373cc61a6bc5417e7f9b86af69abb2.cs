namespace GymAppBackend.Infrastructure.CorsPolicy;

internal sealed class CorsOptions
{
    public IEnumerable<string> AllowedOrigins { get; set; }
}