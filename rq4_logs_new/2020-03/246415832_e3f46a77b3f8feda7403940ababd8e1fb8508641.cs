using System;
using System.IO;
using GzipApplication.ChunkedReader;
using GzipApplication.Compressor;
using GZipApplication.Tests.TestData;
using Xunit;

namespace GZipApplication.Tests
{
    public class GZipIntegrationTests
    {
        [MemberData(nameof(GetBufferData))]
        [Theory]
        public void OriginalDataMatchesDecompressed(UncompressedData dataToCompress)
        {
            var compressedData = GetCompressedData(dataToCompress.Data);

            var decompressedBytes = GetDecompressData(compressedData);

            Assert.Equal(dataToCompress.Data, decompressedBytes);
        }

        private static byte[] GetCompressedData(byte[] dataToCompress)
        {
            var gZipCompressor = new GZipCompressor();

            var outputStream = new MemoryStream();

            gZipCompressor.Execute(new MemoryStream(dataToCompress), outputStream);

            var compressedResult = outputStream.ToArray();

            return compressedResult;
        }

        private static byte[] GetDecompressData(byte[] compressedData)
        {
            var gZipDecompressor = new GZipDecompressor();

            var decompressedStream = new MemoryStream();

            gZipDecompressor.Execute(new MemoryStream(compressedData), decompressedStream);

            var decompressedBytes = decompressedStream.ToArray();

            return decompressedBytes;
        }


        public static TheoryData<UncompressedData> GetBufferData() =>
            new TheoryData<UncompressedData>
            {
                DataSizeTwiceBiggerThanBuffer,
                DataSizeBiggerThanBufferByOne,
                DataSizeTwiceSmallerThanBuffer,
                DataSizeSmallerThanBufferByOne,
                DataSizeEqualsOneByte
            };

        private static UncompressedData DataSizeTwiceBiggerThanBuffer =>
            GetHighlyCompressibleData(FixLengthChunkedReader.DefaultBufferSizeInBytes * 2);

        private static UncompressedData DataSizeBiggerThanBufferByOne =>
            GetHighlyCompressibleData(FixLengthChunkedReader.DefaultBufferSizeInBytes + 1);

        private static UncompressedData DataSizeTwiceSmallerThanBuffer =>
            GetHighlyCompressibleData(FixLengthChunkedReader.DefaultBufferSizeInBytes / 2);

        private static UncompressedData DataSizeSmallerThanBufferByOne =>
            GetHighlyCompressibleData(FixLengthChunkedReader.DefaultBufferSizeInBytes - 1);

        private static UncompressedData DataSizeEqualsOneByte =>
            GetHighlyCompressibleData(1);


        private static UncompressedData GetHighlyCompressibleData(int size)
        {
            var bytes = new byte[size];
            
            return new UncompressedData(bytes);
        }
    }
}