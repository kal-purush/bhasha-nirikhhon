using System;
using GrpcData = Hsnr;
using Module.Hsnr.Timetable.Data;

namespace Module.Hsnr.Extensions
{
    public static class GrpcConversionExtensions
    {
        public static GrpcData.TimetableResponse.Types.Timetable ToGrpc(this Timetable.Data.Timetable timetable)
        {
            return new GrpcData.TimetableResponse.Types.Timetable();
        }

        public static GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay ToGrpc(this Timetable.Data.WeekDay weekDay)
        {
            return new GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay();
        }
        
        public static GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay.Types.Subject ToGrpc(this Timetable.Data.Subject subject)
        {
            return new GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay.Types.Subject();
        }

        public static GrpcData.CalenderType ToGrpc(
            this Timetable.Data.CalendarType calendarType)
        {
            switch (calendarType)
            {
                case CalendarType.Lecturer:
                    return GrpcData.CalenderType.Lecturer;
                case CalendarType.Room:
                    return GrpcData.CalenderType.Room;
                case CalendarType.BranchOfStudy:
                    return GrpcData.CalenderType.BranchOfStudy;
                default:
                    throw new InvalidOperationException("Missing calender type");
            }
        }

        public static GrpcData.SemesterType ToGrpc(
            this Timetable.Data.SemesterType semesterType)
        {
            switch (semesterType)
            {
                case  SemesterType.SummerSemester:
                    return GrpcData.SemesterType.SummerSemester;
                case SemesterType.WinterSemester:
                    return GrpcData.SemesterType.WinterSemester;
                default:
                    throw new InvalidOperationException("Missing semester type");
            }
        }

        public static GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay.Types.Days ToGrpc(this Timetable.Data.Days day)
        {
            switch (day)
            {
                case Days.Monday:
                    return GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay.Types.Days.Monday;
                case Days.Tuesday:
                    return GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay.Types.Days.Tuesday;
                case Days.Wednesday:
                    return GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay.Types.Days.Wednesday;
                case Days.Thursday:
                    return GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay.Types.Days.Thursday;
                case Days.Friday:
                    return GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay.Types.Days.Friday;
                case Days.Saturday:
                    return GrpcData.TimetableResponse.Types.Timetable.Types.WeekDay.Types.Days.Saturday;
                default:
                    throw new InvalidOperationException("Missing day");
            }
        }
    }
}