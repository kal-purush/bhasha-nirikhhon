using IO.Thermo;
using NUnit.Framework;
using System.Linq;

namespace Test
{
    [TestFixture]
    public sealed class ThermoTestFixture
    {
        [Test]
        public void TestThermo()
        {
            ThermoRawFile a = new ThermoRawFile(@"E:\Stefan\data\ToyData\Shew_246a_LCQa_15Oct04_Andro_0904-2_4-20.RAW");
            a.Open();
            Assert.AreEqual(3316, a.LastSpectrumNumber);
        }
    }
}