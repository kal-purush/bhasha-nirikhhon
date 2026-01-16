using System;
using System.IO;
using System.Net.Mime;

using NUnit.Framework;

namespace Ridavei.FileCryption.Tests
{
    [TestFixture]
    public abstract class AFileCryptionBuilderTestsBase
    {
        protected Func<object, ContentType, string, Stream> CryptionMethod;
        protected Func<Func<Stream, ContentType, string, Stream>, AFileCryptionBuilderBase> SetCryptionMethod;
        protected AFileCryptionBuilderBase Builder;
        
        [Test]
        public void Cryption_NoFileLoadMethodSet__RaisesException()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                CryptionMethod(string.Empty, null, string.Empty);
            });
        }

        [Test]
        public void Cryption_NoCryptionMethodSet__RaisesException()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                Builder.SetFileLoaderMethod(TestFileLoadMethod);
                CryptionMethod(string.Empty, null, string.Empty);
            });
        }

        [Test]
        public void Cryption_NullFileInfo__RaisesException()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                Builder.SetFileLoaderMethod(TestNullFileLoadMethod);
                SetCryptionMethod(TestCryptionMethod);
                CryptionMethod(null, null, string.Empty);
            });
        }

        [Test]
        public void Cryption_FileLoadMethodReturnsNull__RaisesException()
        {
            Assert.Throws<FileNotFoundException>(() =>
            {
                Builder.SetFileLoaderMethod(TestNullFileLoadMethod);
                SetCryptionMethod(TestCryptionMethod);
                CryptionMethod("test", null, string.Empty);
            });
        }

        [Test]
        public void Cryption__ReturnsStream()
        {
            Assert.DoesNotThrow(() =>
            {
                Builder.SetFileLoaderMethod(TestFileLoadMethod);
                SetCryptionMethod(TestCryptionMethod);
                using (Stream stream = CryptionMethod("test", null, string.Empty))
                {
                    Assert.IsNotNull(stream);
                }
            });
        }

        private static Stream TestFileLoadMethod(object fileInfo)
        {
            return new MemoryStream();
        }

        private static Stream TestNullFileLoadMethod(object fileInfo)
        {
            return null;
        }

        private static Stream TestCryptionMethod(Stream file, ContentType contentType, string password)
        {
            return new MemoryStream();
        }
    }
}