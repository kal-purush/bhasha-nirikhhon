using System;
using System.Collections.Generic;
using Xunit;
using SolarLunarName.Standard.ApplicationServices;
using System.Linq;
using SolarLunarName.SharedTypes.Models;
using SolarLunarName.Standard.RestServices.LocalJson;
using System.Net.Http;

namespace SolarLunarName.Standard.Tests.RemoteJson
{
    abstract public class JsonClient_ConstructorShould
    {   

        public abstract void Constructor(string input);
        
        protected HttpClient _client = new System.Net.Http.HttpClient();

        [Fact]
        public void Constructor_Should_Not_Throw_Expection()
        {
            // TODO Should run a local server
            Constructor("https://www.google.com");
            
        }
        [Fact]
        public void Constructor_Should_Throw_Expection ()
        {   
            Assert.Throws<ArgumentException>(() => Constructor("notAWebAddress"));
            
        }

    }
    
}