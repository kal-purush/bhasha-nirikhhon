using comp1640_dotnet.Services.Interfaces;
using Amazon.S3.Model;
using Amazon.S3;
using comp1640_dotnet.DTOs.Responses;
using Amazon;
using comp1640_dotnet.Models;

namespace comp1640_dotnet.Services
{
	public class S3Service: IS3Service
	{
		private readonly IConfiguration _configuration;

		public S3Service(IConfiguration configuration)
		{
			_configuration = configuration;
		}

		public async Task<PreSignedUrlResponse> GetPreSignedUrl(string path)
		{
			var amazonAccessKey = _configuration["AWS:ACCESS_KEY"];
			var amazonSecretKey = _configuration["AWS:SECRET_KEY"];
			var amazonBucketName = _configuration["AWS:BUCKET_NAME"];
			string fileName = Guid.NewGuid().ToString();

			IAmazonS3 client = new AmazonS3Client(amazonAccessKey, amazonSecretKey, RegionEndpoint.APSoutheast1);

			GetPreSignedUrlRequest request = new()
			{
				BucketName = amazonBucketName,
				Key = path + fileName + ".jpg",
				Expires = DateTime.Now.AddMinutes(20),
				Verb = HttpVerb.PUT,
			};

			string preSignedUrl = client.GetPreSignedURL(request);

			PreSignedUrlResponse preSignedUrlResponse = new()
			{
				FileName = fileName + ".jpg",
				PreSignedUrl = preSignedUrl,
			};
			return preSignedUrlResponse;
		}
	}
}