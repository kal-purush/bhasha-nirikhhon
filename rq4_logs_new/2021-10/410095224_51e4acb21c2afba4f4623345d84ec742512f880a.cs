using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Rido.IoTHubClient.Tests
{
    public class SasAuthV2Fixture
    {
        [Fact]
        public void GenerateCredentials()
        {
            (string u, byte[] p) = SasAuthV2.GenerateHubSasCredentials("rido", "d1", "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=", 5);
            string pb64 = Convert.ToBase64String(p);
            Console.WriteLine(u);
            Console.WriteLine(pb64);
            Assert.StartsWith("av=2021-06-30-preview", u);
            Assert.Equal(44, pb64.Length);

        }
    }
}