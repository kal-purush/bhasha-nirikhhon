using NUnit.Framework;

namespace DateTest.Test
{
    public class Tests
    {
        private Date[] _dates = 
        {
            new Date(10,10,2021),
            new Date(9, 8, 2018),
            new Date(10, 10, 2014),
            new Date(10, 10, 2012),
            new Date(10, 10, 2011),
            new Date(10, 10, 2008)
        };

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void Date_CheckSetMethods()
        {
            _dates[0].SetDay(1);
            _dates[0].SetMonth(1);
            _dates[0].SetYear(2022);           
            Assert.AreEqual(_dates[0].GetDay(), 1, 0.001, "Entry setter has not been saved correctly");
            Assert.AreEqual(_dates[0].GetMonth(), 1, 0.001, "Entry setter has not been saved correctly");
            Assert.AreEqual(_dates[0].GetYear(), 2022, 0.001, "Entry setter has not been saved correctly");                       
        }

        [Test]
        public void Date_CheckSetupEntryGetMethods()
        {
            Assert.AreEqual(_dates[1].GetDay(), 9, 0.001, "Entry has not been saved correctly");
            Assert.AreEqual(_dates[1].GetMonth(), 8, 0.001, "Entry has not been saved correctly");
            Assert.AreEqual(_dates[1].GetYear(), 2018, 0.001, "Entry has not been saved correctly");
        }

        [Test]
        public void Date_EnterWrongDate()
        {
            _dates[3].SetDay(0);
            _dates[3].SetMonth(14);
            _dates[3].SetYear(-22);
            Assert.AreEqual(_dates[3].GetDay(), 1, 0.001, "Entry setter accepted value out of range ");
            Assert.AreEqual(_dates[3].GetMonth(), 12, 0.001, "Entry setter accepted value out of range");
            Assert.AreEqual(_dates[3].GetYear(), 2022, 0.001, "Entry setter accepted value out of range");
        }

        [Test]
        public void Date_CheckReport()
        {
            Assert.AreEqual(_dates[2].DisplayDate(), "10 / 10 / 2014", "Report method does not work correctly");
        }
    }
}