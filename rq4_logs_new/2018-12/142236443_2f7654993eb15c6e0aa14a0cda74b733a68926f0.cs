using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace NexioPaySample
{
    class ClientSideToken
    {
        private String OneTimeToken_expiration = "";
        private String OneTimeToken_token = "";

        public String ClientSideTokenProcess()
        {

            try
            {
                //Get OneTimeToken
                OneTimeToken_token = GetOneTimeToken();

                //Load public key provided by Nexio from nexiopub.key file in current folder
                RSACryptoServiceProvider RSAProvider = PemKeyUtils.GetPublicRSAProviderFromPemFile(@"./nexiopub.key");

                //the full card number accepted through your form, convert it to byte array
                byte[] CardNumberToBeEncrypted = System.Text.Encoding.UTF8.GetBytes("4111111111111111"); 

                //Encrypt the card number with Nexio public key and covert it to base 64 string
                String CardNumberEncryption = Convert.ToBase64String(RSAProvider.Encrypt(CardNumberToBeEncrypted, false));

                //send save card command
                string url = "https://api.nexiopaysandbox.com/pay/v3/saveCard";

                String postdata = "{\"token\":\"" + OneTimeToken_token +
                              "\",\"card\":{\"encryptedNumber\":\"" + CardNumberEncryption +
                              "\",\"expirationMonth\":\"" + "1" +
                              "\",\"expirationYear\":\"" + "2021" +
                              "\",\"cardHolderName\":\"" + "Test Card" +
                              "\",\"securityCode\":\"" + "123" + "\"}}";

                HttpWebRequest request = WebRequest.Create(url) as HttpWebRequest;

                //set configurations
                request.ProtocolVersion = HttpVersion.Version11;
                request.KeepAlive = false;
                request.Method = "POST";
                request.ContentType = "application/json";
                request.Referer = null;
                request.AllowAutoRedirect = true;
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                ServicePointManager.CheckCertificateRevocationList = true;
                request.Accept = "*/*";

                byte[] data = null;
                data = Encoding.UTF8.GetBytes(postdata);

                //send post data to Nexio
                Stream newStream = request.GetRequestStream();
                newStream.Write(data, 0, data.Length);
                newStream.Close();

                //get response from Nexio
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                Stream stream = response.GetResponseStream();

                string result = string.Empty;
                using (StreamReader sr = new StreamReader(stream))
                {
                    result = sr.ReadToEnd();
                }

                return result;
            }
            catch (Exception e)
            {
                throw e;
            }

        }

        
        private String GetOneTimeToken()
        {
            try
            {
                string url = "https://api.nexiopaysandbox.com/pay/v3/token";

                String postData = "{\"data\":{\"currency\":\"USD\"}, \"customer\":{\"firstName\":\"Test\",\"lastName\":\"Card\"}}";

                HttpWebRequest request = WebRequest.Create(url) as HttpWebRequest;

                //set configurations
                request.ProtocolVersion = HttpVersion.Version11;
                request.KeepAlive = false;
                request.Method = "POST";
                request.ContentType = "application/json";
                request.Referer = null;
                request.Headers["Authorization"] = BasicAuth.basicauth;
                request.AllowAutoRedirect = true;
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                ServicePointManager.CheckCertificateRevocationList = true;
                request.Accept = "*/*";

                byte[] data = null;
                data = Encoding.UTF8.GetBytes(postData);

                //send post data to server
                Stream newStream = request.GetRequestStream();
                newStream.Write(data, 0, data.Length);
                newStream.Close();

                //get response from server
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                Stream stream = response.GetResponseStream();

                string result = string.Empty;
                using (StreamReader sr = new StreamReader(stream))
                {
                    result = sr.ReadToEnd();
                }


                parseOneTimeTokenResponse(result);

                return OneTimeToken_token;
            }
            catch (Exception e)
            {
                throw e;
            }
        }






        private void parseOneTimeTokenResponse(String JSONString)
        {
            DataContractJsonSerializer serializer1 = new DataContractJsonSerializer(typeof(OneTimeTokenResponse));

            OneTimeTokenResponse response = null;

            try
            {
                response = Deserialize<OneTimeTokenResponse>(JSONString);
            }
            catch (Exception e)
            {
                Console.WriteLine("Parsing response get Exception: " + e.Message);
                return;
            }

            OneTimeToken_token = response.token;
            OneTimeToken_expiration = response.expiration;

        }

        private T Deserialize<T>(string json)
        {
            var instance = Activator.CreateInstance<T>();
            using (var ms = new MemoryStream(Encoding.Unicode.GetBytes(json)))
            {
                try
                {
                    var serializer = new DataContractJsonSerializer(instance.GetType());
                    return (T)serializer.ReadObject(ms);
                }
                catch (Exception e)
                {
                    throw e;
                }

            }
        }
    }

    /*
    //structure of One Time Token response
    [DataContract]
    class OneTimeTokenResponse
    {
        [DataMember(Name = "token")]
        public String token { get; set; }

        [DataMember(Name = "expiration")]
        public String expiration { get; set; }
    }*/
    

}