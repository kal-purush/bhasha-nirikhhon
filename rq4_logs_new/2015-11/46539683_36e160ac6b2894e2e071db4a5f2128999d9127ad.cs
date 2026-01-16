using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading.Tasks;

namespace DynamicEncryptionWithDRM {
   public static class CastLabs {
      private static string _merchant = ConfigurationManager.AppSettings["CastLabs.Merchant"];
      private static string _userName = ConfigurationManager.AppSettings["CastLabs.UserName"];
      private static string _password = ConfigurationManager.AppSettings["CastLabs.Password"];

      public static bool DeleteKey(Guid keyId) {
         string keyIdAsHexString = keyId.ToString("N");

         string service = string.Format("/keyId/{0}", keyIdAsHexString);
         var ticket = PostKey(service);

         var myRequest = (HttpWebRequest)WebRequest.Create(string.Format("http://fe.staging.drmtoday.com/frontend/rest/keys/v1/cenc/merchant/{0}/key/keyId/{1}?ticket={2}", _merchant, keyIdAsHexString, ticket));
         myRequest.Method = "DELETE";

         bool result = false;
         try {
            var response = myRequest.GetResponse();
            result = true;
         } catch (WebException) {
            // 409 = key does not exist
            // 401 = not authorized
            // 412 = Precondition failed 

            result = false;
         } catch (Exception) {
            result = false;
         }

         return result;
      }

      private static string ConvertKeyIdToB64(Guid keyId) {
         byte[] keyIdAsString = StringToByteArray(keyId.ToString("N"));
         string keyIdAsBase64 = Convert.ToBase64String(keyIdAsString);

         return keyIdAsBase64;
      }

      public static string IngestKey(string assetId, string variantId, string key, Guid keyId, string algorithm = "AES", string streamType = "VIDEO_AUDIO") {
         var ticket = PostKey();
         Utils.WriteLine(string.Format("Login Redirect URL: {0}", ticket));

         string keyIdAsBase64 = ConvertKeyIdToB64(keyId);
         IngestObject castlabObject = new IngestObject {
            Assets = new List<IngestAsset> {
               //fairplay
               new IngestAsset {
                 Type = "FAIRPLAY",
                  AssetId = string.Format("{0}-fp",assetId),
                  VariantId = variantId,
                 IngestKeys = new List<IngestKey> {
                    new IngestKey {
                        StreamType = streamType,
                        Algorithm = algorithm,
                        Key = key,
                        Iv = keyIdAsBase64,
                    }
                 }
               },
               //widevine
               new IngestAsset {
                 Type = "CENC",
                  AssetId = assetId,
                  VariantId = variantId,
                 IngestKeys = new List<IngestKey> {
                    new IngestKey {
                        KeyId = keyIdAsBase64,
                        StreamType = streamType,
                        Algorithm = algorithm,
                        Key= key
                    }
                 }
               }
            }

         };


         string postData = string.Empty;
         using (var stream = new MemoryStream()) {
            new DataContractJsonSerializer(typeof(IngestObject)).WriteObject(stream, castlabObject);
            stream.Position = 0;
            using (StreamReader sr = new StreamReader(stream)) {
               postData = sr.ReadToEnd();
            }
         }

         Utils.WriteLine(string.Format("Cast Lab Inject Object: {0}", postData));
         byte[] data = Encoding.ASCII.GetBytes(postData);
         ///frontend/api/keys/v2/ingest/your_merchant_id?ticket=cas_ticket
         var myRequest = (HttpWebRequest)WebRequest.Create(string.Format("http://fe.staging.drmtoday.com/frontend/api/keys/v2/ingest/{0}?ticket={1}", _merchant, ticket));
         myRequest.Method = "POST";
         myRequest.ContentType = "application/json";
         myRequest.ContentLength = data.Length;
         using (var newStream = myRequest.GetRequestStream()) {
            newStream.Write(data, 0, data.Length);
         }

         try {
            var response = myRequest.GetResponse();


            string result = string.Empty;
            using (var responseStream = response.GetResponseStream()) {
               using (var responseReader = new StreamReader(responseStream)) {
                  result = responseReader.ReadToEnd();
               }
            }

            Utils.WriteLine();
            Utils.WriteLine(string.Format("Cast Lab Inject Response: {0}", result));
            Utils.WriteLine();
            return result;
         }
         catch(Exception ex) {
            Utils.WriteLine(ex);
            throw;
         }
      }

      private static string GetCLTicket() {
         var postData = string.Format("merchant={0}&username={1}&password={2}", _merchant, _userName, _password);
         byte[] data = Encoding.ASCII.GetBytes(postData);

         var myRequest = (HttpWebRequest)WebRequest.Create("https://auth.staging.drmtoday.com/cas/v1/tickets");
         myRequest.Method = "POST";
         myRequest.ContentType = "application/x-www-form-urlencoded";
         myRequest.ContentLength = data.Length;
         using (var newStream = myRequest.GetRequestStream()) {
            newStream.Write(data, 0, data.Length);
         }

         var response = myRequest.GetResponse();
         var location = response.Headers["location"];

         return location;
      }

      private static string PostKey(string service = "") {
         var location = GetCLTicket();

         var postData = string.Format("service=https://fe.staging.drmtoday.com/frontend/rest/keys/v1/cenc/merchant/{0}/key{1}", _merchant, service);
         byte[] data = Encoding.ASCII.GetBytes(postData);

         var myRequest = (HttpWebRequest)WebRequest.Create(location);
         myRequest.Method = "POST";
         myRequest.ContentType = "application/x-www-form-urlencoded";
         myRequest.ContentLength = data.Length;
         using (var newStream = myRequest.GetRequestStream()) {
            newStream.Write(data, 0, data.Length);
         }

         var response = myRequest.GetResponse();
         string result = string.Empty;
         using (var responseStream = response.GetResponseStream()) {
            using (var responseReader = new StreamReader(responseStream)) {
               result = responseReader.ReadToEnd();
            }
         }

         return result;
      }

      private static byte[] StringToByteArray(string hex) {
         return Enumerable.Range(0, hex.Length)
                          .Where(x => x % 2 == 0)
                          .Select(x => Convert.ToByte(hex.Substring(x, 2), 16))
                          .ToArray();
      }
   }
}