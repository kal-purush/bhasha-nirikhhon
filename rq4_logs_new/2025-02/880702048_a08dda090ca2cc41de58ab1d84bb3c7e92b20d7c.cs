using Amazon.S3;
using FilesService.Core.Options;

namespace FilesService.Builders;

public static class AmazonS3Builder
{
    public static IServiceCollection AddAmazonS3(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var minioOptions = configuration.GetSection(MinioOptions.MINIO).Get<MinioOptions>()
                           ?? throw new Exception("Ошибка со строкой подключения minio. Проверьте конфигурацию");
        
        services.AddSingleton<IAmazonS3>(_ =>
        {
            var config = new AmazonS3Config()
            { 
                ServiceURL = minioOptions.EndPoint,
                ForcePathStyle = true,
                UseHttp = true,
            };
            return new AmazonS3Client(
                minioOptions.AccessKey,
                minioOptions.SecretKey,
                config);
        });
        
        return services;
    }
}