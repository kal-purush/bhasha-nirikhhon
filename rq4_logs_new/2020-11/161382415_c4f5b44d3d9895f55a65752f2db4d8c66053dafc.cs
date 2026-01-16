using System;
using System.Collections.Generic;
using Xunit;
using SolarLunarName.Standard.ApplicationServices;
using System.Linq;
using SolarLunarName.SharedTypes.Models;
using SolarLunarName.Standard.RestServices.LocalJson;

namespace SolarLunarName.Standard.Tests
{
    abstract public class LocalJsonClient_ConstructorShould
    {   

        public abstract void Constructor(string input);

        [Fact]
        public void Constructor_Should_Not_Throw_Expection()
        {
            Constructor(System.IO.Directory.GetCurrentDirectory());
            
        }
        [Fact]
        public void Constructor_Should_Throw_Expection ()
        {
            Assert.Throws<ArgumentException>(() => Constructor("/PPPPPPPPPPPPPPPPPPPPPPP"));
            
        }

    }
    
}