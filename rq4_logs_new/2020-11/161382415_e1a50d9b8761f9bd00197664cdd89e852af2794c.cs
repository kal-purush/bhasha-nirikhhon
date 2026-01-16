using System;
using System.Collections.Generic;
using Xunit;
using SolarLunarName.Standard.ApplicationServices;
using System.Linq;
using SolarLunarName.SharedTypes.Models;
using SolarLunarName.Standard.RestServices.RemoteJson;

namespace SolarLunarName.Standard.Tests
{
    public class RemoteJsonClient_Helpers
    {   

        [Fact]
        public void CombinePath_ShouldFormMonthUri()
        {
            Uri test = Helpers.CombinePath("https://domain.com",1710, 10);
            var expected = new Uri(" https://domain.com/1710/10/index.json");
            Assert.Equal(test, expected);
        }

        [Fact]
        public void CombinePath_ShouldFormYearUri()
        {
            Uri test = Helpers.CombinePath("https://domain.com",1710);
            var expected = new Uri(" https://domain.com/1710/index.json");
            Assert.Equal(test, expected);
        }


    }
    
}