using Microsoft.VisualStudio.TestTools.UnitTesting;
using ClassScheduleCalendar;
using System.Collections.Generic;

namespace ClassScheduleCalendarTests
{
    [TestClass]
    public class ClassScheduleTest
    {
        [TestMethod]
        public void TestConvertDays()
        {
            ClassSchedule classSchedule = new ClassSchedule();

            List<int> days = new List<int>() { 1, 3, 6 };
            string output = "MO,WE,SA";

            Assert.AreEqual(output, classSchedule.ConvertDays(days));

            days = new List<int>() { 0, 2, 4, 6 };
            output = "SU,TU,TH,SA";

            Assert.AreEqual(output, classSchedule.ConvertDays(days));
        }
    }
}