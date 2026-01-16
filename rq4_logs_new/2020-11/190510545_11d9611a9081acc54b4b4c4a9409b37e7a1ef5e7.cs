using System.Threading.Tasks;
using OneBlink.SDK.Model;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
namespace OneBlink.SDK
{
  internal class PrefillClient
  {
    OneBlinkApiClient oneBlinkApiClient;
    internal PrefillClient(OneBlinkApiClient oneBlinkApiClient)
    {
        this.oneBlinkApiClient = oneBlinkApiClient;
    }

    internal async Task<PreFillMeta> GetPreFillMeta(int formId) {
        string url = "/forms/" + formId.ToString() + "/pre-fill-credentials";
        return await this.oneBlinkApiClient.PostRequest<PreFillMeta>(url);
    }
    internal async Task<string> PutPreFillData<T>(T preFillData, PreFillMeta preFillMeta) {
        RegionEndpoint regionEndpoint = RegionEndpoint.GetBySystemName(preFillMeta.s3.region);

        SessionAWSCredentials sessionAWSCredentials = new SessionAWSCredentials(
            preFillMeta.credentials.AccessKeyId,
            preFillMeta.credentials.SecretAccessKey,
            preFillMeta.credentials.SessionToken
        );

        AmazonS3Client amazonS3Client = new AmazonS3Client(sessionAWSCredentials, regionEndpoint);

        PutObjectRequest request = new PutObjectRequest
        {
            BucketName = preFillMeta.s3.bucket,
            Key = preFillMeta.s3.key,
            ContentBody = JsonConvert.SerializeObject(preFillData),
            ContentType = "application/json"
        };

        await amazonS3Client.PutObjectAsync(request);

        return preFillMeta.preFillFormDataId;
    }

    internal async Task<string> SetPreFillData<T>(T preFillData, int formId)
    {
        PreFillMeta preFillMeta = await GetPreFillMeta(formId);
        return await PutPreFillData<T>(preFillData, preFillMeta);
    }
  }
}