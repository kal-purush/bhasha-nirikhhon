namespace SolarLunarName.SharedTypes.Exceptions
{
    public class DayDoesNotExistException : DateDoesNotExistException
    {
        private int _year;
        private int _month;
        private int _day;


        private int _daysInMonth;
        public override string Message =>
            $"Date {_year}-{_month}-{_day} does not exist, this month has only {_daysInMonth} Days";

        public DayDoesNotExistException(int Year, int Month, int Day, int DaysInMonth)
        {
            _year = Year;
            _month = Month;
            _day = Day;
            _daysInMonth = DaysInMonth;
        }


    }

}