using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;

namespace VideoConverter.Tests
{
    [TestClass]
    public class ConversionTest
    {
        private static string currentPath = Path.GetFullPath(@"..\..\");

        [ClassCleanup]
        public static void Cleanup()
        {
            File.Delete(currentPath + "\\SampleVideo.mkv");
            File.Delete(currentPath + "\\SampleVideo.mov");
            File.Delete(currentPath + "\\SampleVideo.mpeg");
            File.Delete(currentPath + "\\SampleVideo.ogg");
            File.Delete(currentPath + "\\SampleVideo.flv");
            File.Delete(currentPath + "\\SampleVideo.webm");
        }

        [TestMethod]
        public void TestMkvConversion()
        {
            Converter converter = new Converter();
            bool result = converter.ConvertFile(currentPath + "\\SampleVideo.mp4", 1, 1);
            Assert.AreEqual(true, result);
        }

        [TestMethod]
        public void TestAviConversion()
        {
            Converter converter = new Converter();
            bool result = converter.ConvertFile(currentPath + "\\SampleVideo.mp4", 2, 1);
            Assert.AreEqual(true, result);
        }

        [TestMethod]
        public void TestMovConversion()
        {
            Converter converter = new Converter();
            bool result = converter.ConvertFile(currentPath + "\\SampleVideo.mp4", 3, 1);
            Assert.AreEqual(true, result);
        }

        [TestMethod]
        public void TestMpegConversion()
        {
            Converter converter = new Converter();
            bool result = converter.ConvertFile(currentPath + "\\SampleVideo.mp4", 4, 1);
            Assert.AreEqual(true, result);
        }

        [TestMethod]
        public void TestOggConversion()
        {
            Converter converter = new Converter();
            bool result = converter.ConvertFile(currentPath + "\\SampleVideo.mp4", 5, 1);
            Assert.AreEqual(true, result);
        }

        [TestMethod]
        public void TestFlvConversion()
        {
            Converter converter = new Converter();
            bool result = converter.ConvertFile(currentPath + "\\SampleVideo.mp4", 6, 1);
            Assert.AreEqual(true, result);
        }

        [TestMethod]
        public void TestWebmConversion()
        {
            Converter converter = new Converter();
            bool result = converter.ConvertFile(currentPath + "\\SampleVideo.mp4", 7, 1);
            Assert.AreEqual(true, result);
        }

        [TestMethod]
        public void TestMp4Conversion()
        {
            Converter converter = new Converter();
            bool result = converter.ConvertFile(currentPath + "\\SampleVideo.avi", 0, 3);
            Assert.AreEqual(true, result);
        }

        [TestMethod]
        public void TestConversionFailure()
        {
            Converter converter = new Converter();
            bool result = converter.ConvertFile(currentPath + "\\SampleVideo.docx", 1, 1);
            Assert.AreEqual(false, result);
        }
    }
}