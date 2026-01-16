using Xunit;
using SolarLunarName.Standard.ApplicationServices;
using SolarLunarName.SharedTypes.Models;
using SolarLunarName.SharedTypes.Exceptions;
using SolarLunarName.SharedTypes.Interfaces;
using SolarLunarName.SharedTypes.Primitives;
using System.Collections.Generic;
using System;

namespace SolarLunarName.Standard.Tests
{   
    public class SolarDateParser_ResolveOverflowingDateShould
    {   
        private SolarDateParser dp;
        private ISolarLunarCalendarClient _client;
        

        public SolarDateParser_ResolveOverflowingDateShould(){
            _client = new MockClient();
            dp = new SolarDateParser(_client);

        }

        [Fact]
        private void ResolveOverflowingDateShould_ResolveResolveOverflowingDate_WhenDaysOverFlowMonth(){
            var initialDate = new SolarLunarNameSimple(1700, 0, 20); 
            var nextDate = new SolarLunarNameSimple(1700, 1, 1);
            TestTemplate(initialDate, nextDate);
        }

        [Fact]
        private void ResolveOverflowingDateShould_ResolveResolveOverflowingDate_WhenMonthsOverFlowYear(){
            var initialDate = new SolarLunarNameSimple(1700, 13, 7); 
            var nextDate = new SolarLunarNameSimple(1701, 0, 7);
            TestTemplate(initialDate, nextDate);
        }

        [Fact]
        private void ResolveOverflowingDateShould_ResolveResolveOverflowingDate_WhenDaysOverFlowYear(){
            var initialDate = new SolarLunarNameSimple(1700, 12, 23);
            var nextDate = new SolarLunarNameSimple(1701, 0, 1);
            TestTemplate(initialDate, nextDate);
        }

        [Fact]
        private void ResolveOverflowingDateShould_ResolveResolveOverflowingDate_WhenDaysAndMonthsNotOverFlowMonthOrYear(){
            var initialDate = new SolarLunarNameSimple(1700, 0, 1);
            var nextDate = new SolarLunarNameSimple(1700, 0, 1);
            TestTemplate(initialDate, nextDate);
        }

        [Fact]
        private void ResolveOverflowingDateShould_ResolveResolveOverflowingDate_WhenDaysAndMonthsOverFlowYearAndMonth(){
            var initialDate = new SolarLunarNameSimple(1700, 13, 9);
            var nextDate = new SolarLunarNameSimple(1701, 0, 9);
            TestTemplate(initialDate, nextDate);
        }
        
        private void TestTemplate(SolarLunarNameSimple initialDate, SolarLunarNameSimple nextDate){
            var nextSolarLunarDate = dp.ResolveOverflowingDate(initialDate);
            
            var result = 
                nextSolarLunarDate.Year == nextDate.Year &&
                nextSolarLunarDate.LunarMonth == nextDate.LunarMonth &&
                nextSolarLunarDate.LunarDay == nextDate.LunarDay;

            Assert.True(result, "Should return expected data.");

        }

        //TODO This is not really a test of ResolveOverflowingDate which does not throw exceptions of its own
        // This may be covered by tests on its dependency the calendar client
        private void ExceptionTemplate(int year, int lunarMonth, int lunarDay){
            var solarLunarDate = new SolarLunarNameSimple(year, lunarMonth, lunarDay);
            Assert.Throws<YearOutOfRangeException>(() => dp.ResolveOverflowingDate(solarLunarDate));
        }


        [Fact]
        public void ResolveOverflowingDateShould_YearTooHigh_ThrowException()
        {   
            ExceptionTemplate(2081,13,3);
        }

    }
    
}