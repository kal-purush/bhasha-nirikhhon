using Catcheap.API.Models.ScooterModels;

namespace Catcheap.Tests.CatcheapApi
{
    public class ScooterChargeTest
    {
        [Fact]
        public void CompareToTest()
        {
            ScooterCharge charge1 = new ScooterCharge();
            ScooterCharge charge2 = new ScooterCharge();

            charge1.StartOfCharge = new DateTime(2022, 12, 13, 12, 0, 0);
            charge2.StartOfCharge = new DateTime(2022, 12, 12, 12, 0, 0);

            Assert.Equal(1, charge1.CompareTo(charge2));
        }
    }
}