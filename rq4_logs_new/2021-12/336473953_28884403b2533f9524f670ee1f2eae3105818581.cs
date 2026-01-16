using Macquarie.Handbook.Data.Transcript;
using Macquarie.Handbook.Data.Transcript.Facts;
using Macquarie.Handbook.Data.Unit.Prerequisites.Facts;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Macquarie.Handbook.Data.Unit.Prerequisites.Parser;

namespace MQHandbookLib_Tests.Tests.Macquarie.Handbook.Data.Unit.Prerequisites.Parser
{
    [TestClass]
    public class TestUnitFactParser
    {
        [TestMethod]
        public void TestBasicParser() {
            UnitRequirementFact desiredResult = new(new UnitFact()
            {
                UnitCode = "COMP1010",
                Results = new(null, EnumGrade.HighDistinction)
            });

            var parser = new UnitFactParser();
            
            string comp1010HD = "COMP1010(HD)";
            var result = parser.Parse(comp1010HD) as UnitRequirementFact;

            Assert.AreEqual(desiredResult.RequiredUnit.UnitCode, result.RequiredUnit.UnitCode);
            Assert.AreEqual(desiredResult.RequiredUnit.Grade, result.RequiredUnit.Grade);

            ///
            string comp1010HD2 = "COMP1010 (HD)";
            result = parser.Parse(comp1010HD2) as UnitRequirementFact;
            
            Assert.AreEqual(desiredResult.RequiredUnit.UnitCode, result.RequiredUnit.UnitCode);
            Assert.AreEqual(desiredResult.RequiredUnit.Grade, result.RequiredUnit.Grade);
        }
    }
}