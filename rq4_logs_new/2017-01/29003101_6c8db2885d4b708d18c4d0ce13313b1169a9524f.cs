using NUnit.Framework;

namespace TabulateSmarterTestContentPackage.Tests
{
    [TestFixture]
    public class ImgTagAltAttributeTests
    {
        private readonly Tabulator tabulator = new Tabulator();

        private void RunValidation(string element, bool isTrue = true)
        {
            // act
            var result = tabulator.ReportMissingImgAltTags(element);

            // assert
            if (isTrue)
            {
                Assert.IsTrue(result);
            }
            else
            {
                Assert.IsFalse(result);
            }
        }

        [Test]
        public void ValidImgTagWithAttributeShouldReturnFalse()
        {
            // arrange
            const string element = "<img id = \"item_1434_graphics1\" src = \"item_1434_v45_graphics1_png256.png\" " +
                                   "width = \"888\" height = \"865\" style = \"vertical-align:baseline;\" alt=\"graphics1\" />";

            //act/assert
            RunValidation(element, false);
        }

        [Test]
        public void ValidImgTagWithEmptyAttributeShouldReturnTrue()
        {
            // arrange
            const string element = "<img id = \"item_1434_graphics1\" src = \"item_1434_v45_graphics1_png256.png\" " +
                                   "width = \"888\" height = \"865\" style = \"vertical-align:baseline;\" alt=\"\" />";

            //act/assert
            RunValidation(element);
        }

        [Test]
        public void ValidImgTagWithEmptyAttributeSingleQuoteShouldReturnTrue()
        {
            // arrange
            const string element = "<img id = \"item_1434_graphics1\" src = \"item_1434_v45_graphics1_png256.png\" " +
                                   "width = \"888\" height = \"865\" style = \"vertical-align:baseline;\" alt=\'\' />";

            //act/assert
            RunValidation(element);
        }

        [Test]
        public void ValidImgTagWithNoAttributeShouldReturnTrue()
        {
            // arrange
            const string element = "<img id = \"item_1434_graphics1\" src = \"item_1434_v45_graphics1_png256.png\" " +
                                   "width = \"888\" height = \"865\" style = \"vertical-align:baseline;\" />";

            //act/assert
            RunValidation(element);
        }

        [Test]
        public void ValidImgTagWithAttributeAndSpacesShouldReturnFalse()
        {
            // arrange
            const string element = "< img id = \"item_1434_graphics1\" src = \"item_1434_v45_graphics1_png256.png\" " +
                                   "width = \"888\" height = \"865\" style = \"vertical-align:baseline;\" alt= \" item1 \" />";

            //act/assert
            RunValidation(element, false);
        }

        [Test]
        public void ValidImgTagWithAttributeAndTabsShouldReturnFalse()
        {
            // arrange
            const string element = "<   img id = \"item_1434_graphics1\" src = \"item_1434_v45_graphics1_png256.png\" " +
                                   "width = \"888\" height = \"865\" style = \"vertical-align:baseline;\" alt= \" item1 \" />";

            //act/assert
            RunValidation(element, false);
        }

        [Test]
        public void ValidImgTagWithAttributeAndNoiseShouldReturnFalse()
        {
            // arrange
            const string element = "12121323FFHFDGF&&&<img id = \"item_1434_graphics1\" src = \"item_1434_v45_graphics1_png256.png\" " +
                                   "width = \"888\" height = \"865\" style = \"vertical-align:baseline;\" alt=\"graphics1\" />TTTADXDFV";

            //act/assert
            RunValidation(element, false);
        }
    }
}