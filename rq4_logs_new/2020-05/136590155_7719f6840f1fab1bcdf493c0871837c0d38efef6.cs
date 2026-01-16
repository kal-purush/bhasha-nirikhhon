using Com.Danliris.Service.Sales.Lib.Helpers;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Com.Danliris.Sales.Test.Helpers
{
   public class APIEndpointTest
    {
        [Fact]
        public void APIEndpoint_Success()
        {
            APIEndpoint.Purchasing = "Purchasing";
            Assert.Equal("Purchasing", APIEndpoint.Purchasing);

            APIEndpoint.AzurePurchasing = "AzurePurchasing";
            Assert.Equal("AzurePurchasing", APIEndpoint.AzurePurchasing);

            APIEndpoint.AzureCore = "AzureCore";
            Assert.Equal("AzureCore", APIEndpoint.AzureCore);

            APIEndpoint.Core = "Core";
            Assert.Equal("Core", APIEndpoint.Core);

            APIEndpoint.StorageAccountName = "StorageAccountName";
            Assert.Equal("StorageAccountName", APIEndpoint.StorageAccountName);

            APIEndpoint.StorageAccountKey = "StorageAccountKey";
            Assert.Equal("StorageAccountKey", APIEndpoint.StorageAccountKey);

            APIEndpoint.ConnectionString = "ConnectionString";
            Assert.Equal("ConnectionString", APIEndpoint.ConnectionString);

            APIEndpoint.Production = "Production";
            Assert.Equal("Production", APIEndpoint.Production);

            APIEndpoint.Finance = "Finance";
            Assert.Equal("Finance", APIEndpoint.Finance);

        }
        }
}