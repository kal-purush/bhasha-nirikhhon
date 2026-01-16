using System;
using System.Collections.Generic;
using Xunit;
using SolarLunarName.Standard.ApplicationServices;
using System.Linq;
using SolarLunarName.SharedTypes.Exceptions;
using SolarLunarName.SharedTypes.Interfaces;
using SolarLunarName.Standard.RestServices.LocalJson;

namespace SolarLunarName.Standard.Tests.JsonClient
{
    abstract public class GetMonthDataExceptions
    {   
        protected ISolarLunarCalendarClient _client;

        [Fact]
        public void Client_Should_Raise_MonthDoesNotExistException(){
          
           Assert.Throws<MonthDoesNotExistException>(() => _client.GetMonthData(1700, 13)); 
        }

        [Fact]
        public void Client_Should_Not_Raise_Exception(){
           try{ 
               _client.GetMonthData(1700, 12);
           }
           catch (Exception e) {
               Assert.True(false, e.Message);
           }

        }

    }
    
}