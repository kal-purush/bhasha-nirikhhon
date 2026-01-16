
using NUnit.Framework;

namespace DG.Code39
{
    [TestFixture]
    internal class Code39Tests
    {
        // encoded "39C"
        private const string Text = "39C";
        private const string Barcode =
            "1000101110111010111011100010101010111000101110101110111010001010100010111011101";

        [TestCase(Barcode,Text)]
        [TestCase($"000{Barcode}00",Text,TestName = "Decode_barcode_with_quiet_zone")]
        [TestCase($"100010110000{Barcode}0000101",Text,TestName = "Decode_barcode_with_trash")]
        public void Decode_barcode(string barcode, string expectedText)
        {
            var code39 = new Code39();

            var decodedText = code39.Decode(barcode);

            Assert.That(decodedText, Is.EqualTo(expectedText));
        }

        [TestCase(Barcode, Text)]
        [TestCase($"000{Barcode}00", Text, TestName = "Decode_reversed_barcode_with_quiet_zone")]
        [TestCase($"100010110000{Barcode}0000101", Text, TestName = "Decode_reversed_barcode_with_trash")]
        public void Decode_reversed_barcode(string barcode, string expectedText)
        {
            Decode_barcode(Code39.Reverse(barcode), expectedText);
        }
    }
}