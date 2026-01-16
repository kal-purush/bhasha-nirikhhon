using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Rido.IoTHubClient.Tests
{
    public class DpsClientFixture
    {
        [Fact]
        public async Task ProvisionWithSas()
        {
            var dpsRes = await DpsClient.ProvisionWithSasAsync("0ne003861C6", "sasdpstest", "l38DGXhjOrdYlqExavXemTBR+QqiAfus9Qp+L1HwuYA=");
            Assert.Equal("rido.azure-devices.net", dpsRes.registrationState.assignedHub);
        }

        [Fact]
        public async Task ProvisionWithCert()
        {
            var dpsRes = await DpsClient.ProvisionWithCertAsync("0ne003861C6", "testdevice.pfx", "1234");
            Assert.Equal("rido.azure-devices.net", dpsRes.registrationState.assignedHub);
        }
    }
}