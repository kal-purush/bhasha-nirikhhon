using System;

namespace TopTrumps.Files.Services
{
    using System.IO;
    using System.Threading.Tasks;
    using Amazon;
    using Amazon.Runtime;
    using Amazon.S3;
    using Amazon.S3.Transfer;
    using Microsoft.Extensions.Configuration;

    public class AwsS3FileService : IFileService
    {
        private readonly IConfiguration _configuration;

        public AwsS3FileService(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task<string> UploadFile(string fileName, Stream fileStream)
        {
            var key = $"{Guid.NewGuid()}.{fileName}";
            var region = RegionEndpoint.GetBySystemName(_configuration["Aws:Region"]);
            var credentials = new BasicAWSCredentials(_configuration["AWS:AccessKey"], _configuration["AWS:SecretKey"]);
            var config = new AmazonS3Config
            {
                RegionEndpoint = region
            };

            using var client = new AmazonS3Client(credentials, config);

            var uploadRequest = new TransferUtilityUploadRequest
            {
                InputStream = fileStream,
                Key = key,
                BucketName = _configuration["AWS:S3BucketName"],
                CannedACL = S3CannedACL.PublicRead
            };

            var fileTransferUtility = new TransferUtility(client);
            await fileTransferUtility.UploadAsync(uploadRequest);

            return GetUrl(key);
        }

        private string GetUrl(string key)
        {
            var bucketName = _configuration["AWS:S3BucketName"];
            var region = _configuration["Aws:Region"];
            return $"https://{bucketName}.s3.{region}.amazonaws.com/{key}";
        }
    }
}