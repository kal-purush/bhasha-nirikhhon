using Catcheap.API.Models.CarModels;
using Catcheap.API.Models.ScooterModels;
using Catcheap.API.Services.CarServices;
using Catcheap.API.Services.ScooterServices;
using Catcheap.Tests.CatcheapApi.MockRepo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Catcheap.Tests.CatcheapApi
{
    public class ScooterChargeServiceTest
    {
        [Fact]
        public void CalculateDurationOfChargeTest()
        {
            ScooterCharge charge = new ScooterCharge();
            charge.StartOfCharge = new DateTime(2022, 12, 18, 12, 0, 0);
            charge.EndOfCharge = new DateTime(2022, 12, 18, 15, 0, 0);
            ScooterChargeService service = new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository());
            TimeSpan rez = service.CalculateDurationOfCharge(charge);
            Assert.True(rez == (new TimeSpan(3, 0, 0)), "Bad");
        }

        [Fact]
        public void CalculateChargedKWhTest()
        {
            ScooterCharge charge = new ScooterCharge();
            charge.StartOfCharge = new DateTime(2022, 12, 18, 12, 0, 0);
            charge.EndOfCharge = new DateTime(2022, 12, 18, 15, 30, 0);
            charge.ChargingPower = 10;
            ScooterChargeService service = new ScooterChargeService(new MockScooterChargeRepository(), new MockNordpoolPriceRepository());
            double rez = service.CalculateChargedKWh(charge);
            Assert.True(rez == 35, "Bad");
        }

        [Fact]
        public void CalculateChargePriceTest()
        {
            MockScooterChargeRepository scooterChargeRepository = new MockScooterChargeRepository();
            ScooterCharge charge = new ScooterCharge();
            charge.StartOfCharge = new DateTime(2022, 12, 18, 12, 0, 0);
            charge.EndOfCharge = new DateTime(2022, 12, 18, 15, 30, 0);
            charge.ChargingPower = 10;
            ScooterChargeService service = new ScooterChargeService(scooterChargeRepository, new MockNordpoolPriceRepository());
            service.CalculateChargePrice(charge);

            double rez = scooterChargeRepository.Charge.ChargingPrice;
            Assert.True(rez == 35, "Bad");
        }
    }
}