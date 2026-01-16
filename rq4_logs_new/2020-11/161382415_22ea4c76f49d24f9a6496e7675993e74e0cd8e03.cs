using Xunit;
using SolarLunarName.Standard.ApplicationServices;
using SolarLunarName.SharedTypes.Models;
using SolarLunarName.SharedTypes.Exceptions;
using SolarLunarName.SharedTypes.Interfaces;
using SolarLunarName.SharedTypes.Primitives;
using System.Collections.Generic;
using System;

namespace SolarLunarName.Standard.Tests.Mocks{
    
    public class SolarLunarCalendarClient: ISolarLunarCalendarClient
    {
        public IList<ILunarSolarCalendarMonth> GetYearData(ValidYear year){
            
            return new List<ILunarSolarCalendarMonth>{
                new LunarSolarCalendarMonth(19, new DateTime(1700,1,1)),
                new LunarSolarCalendarMonth(29,new DateTime(1700,01,20)),
                new LunarSolarCalendarMonth(30,new DateTime(1700,02,18)),
                new LunarSolarCalendarMonth(30,new DateTime(1700,03,20)),
                new LunarSolarCalendarMonth(29,new DateTime(1700,04,19)),
                new LunarSolarCalendarMonth(30,new DateTime(1700,05,18)),
                new LunarSolarCalendarMonth(29,new DateTime(1700,06,17)),
                new LunarSolarCalendarMonth(29,new DateTime(1700,07,16)),
                new LunarSolarCalendarMonth(30,new DateTime(1700,08,14)),
                new LunarSolarCalendarMonth(29,new DateTime(1700,09,13)),
                new LunarSolarCalendarMonth(29,new DateTime(1700,10,12)),
                new LunarSolarCalendarMonth(30,new DateTime(1700,11,10)),
                new LunarSolarCalendarMonth(22,new DateTime(1700,12,10))
            };
        }
        public ILunarSolarCalendarMonth GetMonthData(ValidYear year, ValidLunarMonth month)=> throw new NotImplementedException();
        public IList<DateTime> GetYear(ValidYear year)=> throw new NotImplementedException();
        public ILunarSolarCalendarMonth GetMonthData(int year, int month)=> throw new NotImplementedException();
        public IList<DateTime> GetYear(string year)=> throw new NotImplementedException();
        public IList<ILunarSolarCalendarMonth> GetYearData(string year)=> GetYearData((ValidYear)year);
    }
}