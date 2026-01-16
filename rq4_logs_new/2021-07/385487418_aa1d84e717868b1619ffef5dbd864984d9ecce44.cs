using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;

namespace FlightPlanner.Test
{
    public class Tests
    {
        private FlightList _flightList = new FlightList();

        [SetUp]
        public void Setup()
        {
            _flightList.AddDestination("Zelta Dibens -> Kuja");
            _flightList.AddDestination("Zelta Dibens -> Uzvara");
            _flightList.AddDestination("Uzvara -> Kuja");
            _flightList.AddDestination("Uzvara -> Zelta Dibens");
            _flightList.AddDestination("Zelta Dibens -> Naudaskalns");
            _flightList.AddDestination("Zelta Dibens -> Lielie Mulki");
        }

        [Test]
        public void FlightList_CheckSetupDepartureCities()
        {
            Assert.AreEqual(_flightList.GetDepartureCityByIndex(0), "Zelta Dibens", "Entry has not been saved correctly");
            Assert.AreEqual(_flightList.GetDepartureCityByIndex(1), "Uzvara", "Entry has not been saved correctly");
        }

        [Test]
        public void FlightList_CheckSetupDepartureCityList()
        {
            Assert.AreEqual(_flightList.GetDepartureCities().Count, 2, "Entry has not been saved correctly");
            Assert.AreEqual(_flightList.GetDepartureCities().ElementAt(0), "Zelta Dibens", "Entry has not been saved correctly");
        }

        [Test]
        public void FlightList_CheckSetupArrivalCities()
        {
            Assert.AreEqual(_flightList.GetArrivalCityByIndex(0,"Zelta Dibens"), "Kuja", "Entry has not been saved correctly");
            Assert.AreEqual(_flightList.GetArrivalCityByIndex(1, "Zelta Dibens"), "Uzvara", "Entry has not been saved correctly");
            Assert.AreEqual(_flightList.GetArrivalCityByIndex(2, "Zelta Dibens"), "Naudaskalns", "Entry has not been saved correctly");
            Assert.AreEqual(_flightList.GetArrivalCityByIndex(0, "Uzvara"), "Kuja", "Entry has not been saved correctly");
        }

        [Test]
        public void FlightList_CheckSetupArrivalCityList()
        {
            Assert.AreEqual(_flightList.GetArrivalCities("Zelta Dibens").Count, 4, "Entry has not been saved correctly");
            Assert.AreEqual(_flightList.GetArrivalCities("Zelta Dibens").ElementAt(2), "Naudaskalns", "Entry has not been saved correctly");
        }

        [Test]
        public void FlightList_TheSameEntry()
        {
            _flightList.AddDestination("Zelta Dibens -> Lielie Mulki");
            Assert.AreEqual(_flightList.GetArrivalCities("Zelta Dibens").Count, 4, "The same entry should not be saved");
            Assert.AreEqual(_flightList.GetArrivalCities("Zelta Dibens").ElementAt(2), "Naudaskalns", "The same entry should not be saved");
        }
    }
}