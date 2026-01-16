using System;
using System.Collections.Generic;
using System.Linq;
using SolarLunarName.SharedTypes.Models;
using SolarLunarName.SharedTypes.Interfaces;
using System.IO;

namespace SolarLunarName.Standard.RestServices.Json
{

    public abstract class LunarCalendarClient : ISolarLunarCalendarClient
    {

        protected IList<ILunarSolarCalendarMonth> GetYearData(string year, Stream s)
        {

            SolarLunarName.SharedTypes.Validation.Helpers.ValidateYear(year);

            return Helpers.Deserialize<List<LunarSolarCalendarMonth>>(s)
                                        .Select(month => (ILunarSolarCalendarMonth)month)
                                        .ToList();


        }

        protected ILunarSolarCalendarMonth GetMonthData(int year, int month, Stream s)
        {

            SolarLunarName.SharedTypes.Validation.Helpers.ValidateYear(year);
            SolarLunarName.SharedTypes.Validation.Helpers.ValidateLunarMonth(month);

            try
            {
                return Helpers.Deserialize<LunarSolarCalendarMonth>(s);
            }
            catch (System.IO.DirectoryNotFoundException)
            {

                var monthsInYear = GetYear(year.ToString()).Count();
                throw new SolarLunarName.SharedTypes.Exceptions.MonthDoesNotExistException(year, month, monthsInYear);
            }

        }

        protected IList<DateTime> GetYear(string year, Stream s)
        {
            return GetYearData(year)
                .Select(Month => Month.FirstDay)
                .ToList();
        }
        public IList<DateTime> GetYear(string year){
                return GetYearData(year)
                    .Select(x=> x.FirstDay)
                    .ToList();
        }
        public abstract IList<ILunarSolarCalendarMonth> GetYearData(string year);
        public abstract ILunarSolarCalendarMonth GetMonthData(int year, int month);

        protected virtual IList<ILunarSolarCalendarMonthDetailed> GetYearDataDetailed(string year, Stream s)
        {
            SolarLunarName.SharedTypes.Validation.Helpers.ValidateLunarMonth(year);

            return Helpers.Deserialize<List<LunarSolarCalendarMonthDetailed>>(s)
                .Select(month => (ILunarSolarCalendarMonthDetailed)month)
                .ToList();
            

        }

        protected virtual ILunarSolarCalendarMonthDetailed GetMonthDataDetailed(int year, int month, Stream s)
        {

            SolarLunarName.SharedTypes.Validation.Helpers.ValidateYear(year);
            SolarLunarName.SharedTypes.Validation.Helpers.ValidateLunarMonth(month);

            return Helpers.Deserialize<LunarSolarCalendarMonthDetailed>(s);

        }

    }

}