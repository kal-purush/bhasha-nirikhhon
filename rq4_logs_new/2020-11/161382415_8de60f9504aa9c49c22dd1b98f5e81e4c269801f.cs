using System;
using System.Collections.Generic;
using Xunit;
using SolarLunarName.Standard.ApplicationServices;
using System.Linq;
using SolarLunarName.SharedTypes.Models;

namespace SolarLunarName.Standard.Tests
{
    public class LunarCalendarClient_ConstructorShould
    {   

        [Fact]
        public void Constructor_Should_Not_Throw_Expection ()
        {
            new Standard.RestServices.LocalJson.LunarCalendarClient(System.IO.Directory.GetCurrentDirectory());
            
        }
        [Fact]
        public void Constructor_Should_Throw_Expection ()
        {
            Assert.Throws<ArgumentException>(() =>new Standard.RestServices.LocalJson.LunarCalendarClient("/PPPPPPPPPPPPPPPPPPPPPPP"));
            
        }

    }
    
}