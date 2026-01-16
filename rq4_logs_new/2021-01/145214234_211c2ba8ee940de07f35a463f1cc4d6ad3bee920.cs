using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Server.Text;

namespace Benchmarks.BenchmarkText
{
    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.NetCoreApp50)]
    public class BenchmarkTextEncoding
    {
        private const string text =
            "This is supposed to be a really long text Ƞă¼ÖƄŭȘú😅¥♓ǵDƤĂ😋ġǁ⚕😐'ƿī😪_l" +
            "This is supposed to be a really long text Ƞă¼ÖƄŭȘú😅¥♓ǵDƤĂ😋ġǁ⚕😐'ƿī😪_l" +
            "This is supposed to be a really long text Ƞă¼ÖƄŭȘú😅¥♓ǵDƤĂ😋ġǁ⚕😐'ƿī😪_l" +
            "This is supposed to be a really long text Ƞă¼ÖƄŭȘú😅¥♓ǵDƤĂ😋ġǁ⚕😐'ƿī😪_l" +
            "This is supposed to be a really long text Ƞă¼ÖƄŭȘú😅¥♓ǵDƤĂ😋ġǁ⚕😐'ƿī😪_l";

        [Benchmark]
        public byte[] TestEncodingOldReturnBytes()
        {
            var bytes = TextEncoding.UTF8.GetBytes(text);
            return bytes;
        }

        [Benchmark]
        public byte[] TestEncodingNewReturnBytes()
        {
            var bytes = text.GetBytesUtf8();
            return bytes;
        }
    }
}