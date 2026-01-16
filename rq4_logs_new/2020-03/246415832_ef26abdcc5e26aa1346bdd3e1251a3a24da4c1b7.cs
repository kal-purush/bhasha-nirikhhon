using GzipApplication.Compressor;
using GzipApplication.WorkQueue;

namespace GzipApplication.Constants
{
    public class ApplicationConstants
    {
        /// <summary>
        ///     Buffer calculated in order for compressed chunk to fit ArrayBuffer max array length
        /// </summary>
        public const int BufferSizeInBytes = 896_000;

        public const int GzipStreamHeaderAndFooterInBytes = 25;

        public static readonly int BufferSlots = CpuBoundWorkQueue.ParallelWorkMax * 2;

        public static readonly int CompressedChunkMaxSize =
            BufferSizeInBytes + GzipStreamHeaderAndFooterInBytes + BaseGzipAction
                .CalculateBufferOverhead(BufferSizeInBytes);
    }
}