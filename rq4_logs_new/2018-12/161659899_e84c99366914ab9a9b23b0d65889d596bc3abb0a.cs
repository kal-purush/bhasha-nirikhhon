using InitialPrefabs.Attributes;
using InitialPrefabs.Examples;
using NUnit.Framework;

namespace InitialPrefabs.Tests.Attributes {

    public class ProgressBarAttributeGUITests : BaseAttributeGUITests {

        private ProgressBarAttribute progressBar;
        private const string DefaultField = "defaultFloat";

        [SetUp]
        public override void SetUp() {
            AssignTestObject<ProgressBarExample>();
            progressBar = new ProgressBarAttribute();
        }

        [Test]
        public void ProgressBarIntHeightTest() {
            var pair = RetrievePropertyHeights(DefaultField, "intValue");

            Assert.Greater(pair.Item2, pair.Item1, "The intValue field should have a bigger height than the " +
                    "defaultFloat field!");
        }

        [Test]
        public void ProgressBarFloatHeightTest() {
            var pair = RetrievePropertyHeights(DefaultField, "floatValue");

            Assert.Greater(pair.Item2, pair.Item1, "The floatValue field should have a bigger height than the " +
                    "defaultFloat field!");
        }

        [Test]
        public void InvalidProgressbarHeightTest() {
            var pair = RetrievePropertyHeights(DefaultField, "invalidField");

            Assert.AreEqual(pair.Item1, pair.Item2, "Height mismatch! The heights should not be modified on a non " +
                    "numeric field!");
        }
    }
}