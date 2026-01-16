using FilesService.Core.Options;
using Minio;

namespace FilesService.Builders;

public static class MinioBuilder
{
    public static IServiceCollection SetMinioOptions(
        this IServiceCollection services, IConfiguration configuration)
    {
        var minioOptions = configuration.GetSection(MinioOptions.MINIO).Get<MinioOptions>()
                           ?? throw new Exception("Ошибка со строкой подключения minio. Проверьте конфигурацию.");

        services.AddMinio(options =>
        {
            options.WithEndpoint(minioOptions.EndPoint);
            options.WithCredentials(minioOptions.AccessKey, minioOptions.SecretKey);
            options.WithSSL(minioOptions.WithSSl);
        });
        return services;
    }
}