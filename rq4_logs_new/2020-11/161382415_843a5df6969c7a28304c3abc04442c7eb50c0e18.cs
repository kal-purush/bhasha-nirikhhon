namespace SolarLunarName.SharedTypes.Exceptions
{
    public class MonthDoesNotExistException : DateDoesNotExistException
    {
        private int _year;
        private int _month;
        private int _monthsInYear;

        public override string Message =>
            $"Month: {_month} of Year: {_year} does not exist, this year has only {_monthsInYear} Months 0 - {_monthsInYear - 1}";

        public MonthDoesNotExistException(int Year, int Month, int MonthInYear)
        {
            _year = Year;
            _month = Month;
            _monthsInYear = MonthInYear;
        }


    }

}