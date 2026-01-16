using System;
using System.IO;
using Azure;
using Azure.Security.KeyVault.Certificates;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace KeyVaultIntegrationFunc
{
    [StorageAccount("BlobConnectionString")]
    public class Function1
    {
        private readonly CertificateClient _certificateClient;

        public Function1(CertificateClient certificateClient)
        {
            _certificateClient = certificateClient;
        }

        [FunctionName("Function1")]
        public void Run([BlobTrigger("normal-size/{name}")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            Pageable<CertificateProperties> allCertificates = _certificateClient.GetPropertiesOfCertificates();

            foreach(CertificateProperties props in allCertificates)
            {
                log.LogInformation(props.Name);
            }
        }
    }
}