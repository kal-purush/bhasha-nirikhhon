using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Social.BL
{
    public class AmazonS3Uploader
    {
        //static readonly string bucketUrl = ConfigurationManager.AppSettings["s3Key"];
        static readonly string bucketName = "ozbook";

        public string UploadFile(string image)
        {
            if (string.IsNullOrWhiteSpace(image)) return null;

            var s3Client = new AmazonS3Client(RegionEndpoint.EUCentral1);
            try
            {
                // get image format
                string format = Regex.Match(image, @"^data:image\/([a-zA-Z]+);").Groups[1].Value;

                //remove metadata from image string64
                string result = Regex.Replace(image, @"^data:image\/[a-zA-Z]+;base64,", String.Empty);
                byte[] bytes = Convert.FromBase64String(result);
                string key = Guid.NewGuid() + "." + format;

                using (s3Client)
                {
                    var request = new PutObjectRequest
                    {
                        BucketName = bucketName,
                        CannedACL = S3CannedACL.PublicRead,
                        Key = key
                    };
                    using (var ms = new MemoryStream(bytes))
                    {
                        request.InputStream = ms;
                        var res = s3Client.PutObject(request);
                        return "bucketUrl" + key;

                    }
                }
            }
            catch (Exception ex)
            {
                string a = ex.Message;
                return null;
            }
        }
    }
}