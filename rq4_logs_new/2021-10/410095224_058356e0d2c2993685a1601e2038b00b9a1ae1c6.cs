using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Rido.IoTHubClient
{
    public class DpsClient
    {
        public IMqttClient MqttClient;

        public DpsClient()
        {
            var factory = new MqttFactory();
            MqttClient = factory.CreateMqttClient();

        }
        public async Task<string> ProvisionWithSas(string idScope, string registrationId, string sasKey)
        {
            var resource = $"{idScope}/registrations/{registrationId}";
            var username = $"{resource}/api-version=2019-03-31";
            var password = CreateSasToken(resource, sasKey, TimeSpan.FromMinutes(5));
            Console.WriteLine(username);
            Console.WriteLine(password);
            var options = new MqttClientOptionsBuilder()
                .WithClientId(registrationId)
                .WithTcpServer("global.azure-devices-provisioning.net", 8883)
                .WithCredentials(username, password)
                .WithTls(new MqttClientOptionsBuilderTlsParameters
                {
                    UseTls = true,
                    CertificateValidationHandler = (x) => { return true; },
                    SslProtocol = SslProtocols.Tls12
                })
            .Build();

            await MqttClient.ConnectAsync(options);
            await MqttClient.SubscribeAsync("$dps/registrations/res/#");
            MqttClient.UseApplicationMessageReceivedHandler(e =>
            {
                string msg = string.Empty;
                Console.WriteLine(e.ApplicationMessage.Topic);
                if (e.ApplicationMessage.Payload != null)
                {
                    msg = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                    Console.WriteLine(msg);
                }
                if (e.ApplicationMessage.Topic.StartsWith("$dps/registrations/res/"))
                {

                }
            });
            await MqttClient.PublishAsync("$dps/registrations/PUT/iotdps-register/?$rid=1");
            await Task.Delay(5000);
            return await Task.FromResult("");
        }

        private string BuildExpiresOn(TimeSpan timeToLive)
        {
            DateTime EpochTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            DateTime expiresOn = DateTime.UtcNow.Add(timeToLive);
            TimeSpan secondsFromBaseTime = expiresOn.Subtract(EpochTime);
            long seconds = Convert.ToInt64(secondsFromBaseTime.TotalSeconds, CultureInfo.InvariantCulture);
            return Convert.ToString(seconds, CultureInfo.InvariantCulture);
        }

        private string CreateSasToken(string target, string sasKey, TimeSpan ttl)
        {
            var expiry = BuildExpiresOn(ttl);
            //string audience = WebUtility.UrlEncode(target);
            var sig = Sign($"{target}\n{expiry}", sasKey);
            return $"SharedAccessSignature sr={target}&sig={sig}&se={expiry}&skn=registration";
        }

        protected string Sign(string requestString, string key)
        {
            using var algorithm = new HMACSHA256(Convert.FromBase64String(key));
            return Convert.ToBase64String(algorithm.ComputeHash(Encoding.UTF8.GetBytes(requestString)));
        }

        public async Task<string> ProvisionWithCert(string IdScope, string pfxPath, string pfxPwd)
        {
            return await Task.FromResult("");
        }
    }
}