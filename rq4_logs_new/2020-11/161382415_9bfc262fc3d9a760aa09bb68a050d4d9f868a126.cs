using SolarLunarName.SharedTypes.Exceptions;
using SolarLunarName.SharedTypes.Constants;
using System;

namespace SolarLunarName.SharedTypes.Validation
{
    public static class Helpers
    {
        
        static private void ValidationHelper<Exp>(int value, Range range) where Exp : System.Exception, new()  {
                if (! range.InRange(value))
                {
                    throw new Exp();
                }
        }

        static public void ValidateYear(int value){
             ValidationHelper<YearOutOfRangeException>(value, Ranges.Year);
        }
        static public void ValidateYear(string value){
             ValidateYear(Int32.Parse(value));
        }


        static public void ValidateLunarMonth(int value){
             ValidationHelper<MonthOutOfRangeException>(value, Ranges.Month);
        }

        static public void ValidateLunarMonth(string value){
             ValidateLunarMonth(Int32.Parse(value));
        }


        static public void ValidateLunarDay(int value){
             ValidationHelper<DayOutOfRangeException>(value, Ranges.Day);
        }
        static public void ValidateLunarDay(string value){
             ValidateLunarDay(Int32.Parse(value));
        }

    }



}