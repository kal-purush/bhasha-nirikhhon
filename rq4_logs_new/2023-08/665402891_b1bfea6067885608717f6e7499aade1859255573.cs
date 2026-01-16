using Calendar.Mongo.Db.Models;

namespace Calendar.Api.Validation
{
    public static class EventValidator
    {
        public static CalendarEvent ValidateLocation(this CalendarEvent calendarEvent)
        {
            ArgumentException.ThrowIfNullOrEmpty(calendarEvent.Location);
            return calendarEvent;
        }
        public static CalendarEvent ValidateSeriesId(this CalendarEvent calendarEvent)
        {
            ArgumentNullException.ThrowIfNull(calendarEvent.SeriesId);
            return calendarEvent;
        }
        public static CalendarEvent ValidateCalendarId(this CalendarEvent calendarEvent)
        {
            ArgumentException.ThrowIfNullOrEmpty(calendarEvent.CalendarId);
            return calendarEvent;
        }
        public static CalendarEvent ValidateSeriesTimes(this CalendarEvent calendarEvent)
        {
            if (calendarEvent.Repeat == EventRepeat.None)
                return calendarEvent;
            var endSeries = calendarEvent.EndSeries ?? throw new ArgumentException("If repeat is set, the end of the series must also be set.");

            var eventEnd = (calendarEvent.Start + calendarEvent.Duration);
            var seriesEndDate = new DateTimeOffset(endSeries.Year, endSeries.Month, endSeries.Day, eventEnd.Hour, eventEnd.Minute, eventEnd.Second, TimeSpan.Zero);

            if (seriesEndDate < calendarEvent.Start)
                throw new ArgumentException($"{nameof(calendarEvent.EndSeries)} must be bigger than the {nameof(calendarEvent.Start)} from the event.");

            if (calendarEvent.Repeat == EventRepeat.Daily && seriesEndDate < calendarEvent.Start.AddDays(1))
                throw new ArgumentException($"{nameof(calendarEvent.EndSeries)} must for daily repeat at least one day bigger than event start");

            if (calendarEvent.Repeat == EventRepeat.Weekly && seriesEndDate < calendarEvent.Start.AddDays(7))
                throw new ArgumentException($"{nameof(calendarEvent.EndSeries)} must for weekly repeat at least one week bigger than event start");

            if (calendarEvent.Repeat == EventRepeat.Monthly && seriesEndDate < calendarEvent.Start.AddMonths(1))
                throw new ArgumentException($"{nameof(calendarEvent.EndSeries)} must for monthly repeat at least one month bigger than event start");
            return calendarEvent;
        }
    }
}