namespace CSL;

public class Duration
{
    public int Minutes { get; init; }

    public int Months { get; init; }

    public Duration(int minutes, int months)
    {
        Minutes = minutes;
        Months = months;
    }

    public static Duration FromMinutes(int minutes) => new Duration(minutes, 0);

    public static Duration FromHours(int hours) => FromMinutes(60 * hours);

    public static Duration FromDays(int days) => FromHours(24 * days);

    public static Duration FromWeeks(int weeks) => FromDays(7 * weeks);

    public static Duration FromMonths(int months) => new Duration(0, months);

    public static Duration FromYears(int years) => FromMonths(12 * years);

    public const int MinuteFactor = 1;

    public const int HourFactor = 60 * MinuteFactor;

    public const int DayFactor = 24 * HourFactor;

    public const int WeekFactor = 7 * DayFactor;

    public const int MonthFactor = 1;

    public const int YearFactor = 12 * WeekFactor;
}