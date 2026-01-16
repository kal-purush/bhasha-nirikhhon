using System;
using GzipApplication.Constants;

namespace GzipApplication.Compressor
{
    public static class ArchiveSizeCalculator
    {
        /// <summary>
        ///     Calculates archive max size including GZip overhead and header and footer.
        /// </summary>
        /// <returns>Archive max size in bytes.</returns>
        public static int CalculateArchiveMaxSizeInBytes(int bufferSizeInBytes) =>
            bufferSizeInBytes + ApplicationConstants.GzipStreamHeaderAndFooterInBytes +
            CalculateBufferOverheadInBytes(bufferSizeInBytes);

        private static int CalculateBufferOverheadInBytes(int bufferSizeInBytes)
        {
            const int gzipOverheadOver32KbInBytes = 5;

            var bufferOverhead = (int) Math.Ceiling((double) bufferSizeInBytes / 32) * gzipOverheadOver32KbInBytes;

            return bufferOverhead;
        }
    }
}