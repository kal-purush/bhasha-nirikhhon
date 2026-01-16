using BenchmarkDotNet.Attributes;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImportingProcess
{
    [MarkdownExporterAttribute.GitHub]
    [ShortRunJob]
    [MemoryDiagnoser]
    public class Import02
    {
        const string INPUT_FILE = @"data01.txt";
        const int HEADER_LEN = 9 + 30 * 2;
        const int DETAIL_LEN = 10;
        const int DETAIL_COUNT = 10;
        const int FOOTER_LEN = 39 * 2;

        readonly int _footerOffset = HEADER_LEN + DETAIL_LEN * DETAIL_COUNT;
        readonly int _totalLength = HEADER_LEN + DETAIL_LEN * DETAIL_COUNT + FOOTER_LEN + 2;
        readonly Encoding _enc;
        readonly byte[] _input;

        public Import02()
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            _enc = Encoding.GetEncoding("shift-jis");
            //_input = File.ReadAllBytes(INPUT_FILE);
            _input = Enumerable.Repeat(File.ReadAllBytes(INPUT_FILE), 1000).SelectMany(x => x).ToArray();
        }

        [Benchmark]
        public async Task FirstVersionAsync()
        {
            var comma = _enc.GetBytes(",");
            var newLine = _enc.GetBytes("\n");
            using (var ms = new MemoryStream())
            {
                await foreach (var r in ImportAsync())
                {
                    ms.Write(_enc.GetBytes($"{r.HeaderID},"));
                    ms.Write(_enc.GetBytes($"{r.DetailID:000},"));
                    ms.Write(r.Data.Span);
                    ms.Write(comma);
                    ms.Write(r.Header01.Span);
                    ms.Write(r.Header02.Span);
                    ms.Write(r.Header03.Span);
                    ms.Write(r.Header04.Span);
                    ms.Write(r.Header05.Span);
                    ms.Write(r.Header06.Span);
                    ms.Write(r.Header07.Span);
                    ms.Write(r.Footer01.Span);
                    ms.Write(r.Footer02.Span);
                    ms.Write(r.Footer03.Span);
                    ms.Write(r.Footer04.Span);
                    ms.Write(r.Footer05.Span);
                    ms.Write(r.Footer06.Span);
                    ms.Write(newLine);
                }
            }
        }

        private async IAsyncEnumerable<Row> ImportAsync()
        {
            var lineNum = 1;
            using (var ms = new MemoryStream(_input))
            {
                for (; ; )
                {
                    var buffer = new byte[_totalLength];
                    var len = await ms.ReadAsync(buffer);
                    if (len == 0)
                        break;
                    if (buffer.Last() == (byte)'\r')
                        throw new ApplicationException("LF not found");

                    for (int i = 0; i < DETAIL_COUNT; i++)
                    {
                        yield return new Row(
                            lineNum++,
                            i,
                            buffer.AsMemory()[..HEADER_LEN],
                            buffer.AsMemory().Slice(_footerOffset, FOOTER_LEN),
                            buffer.AsMemory().Slice(_footerOffset, FOOTER_LEN)
                        );
                    }
                }
            }
        }

        public class Row
        {
            readonly int _headerID;
            readonly int _detailID;
            readonly Memory<byte> _header;
            readonly Memory<byte> _detail;
            readonly Memory<byte> _footer;

            public Row(int headerID, int detailID, Memory<byte> header, Memory<byte> detail, Memory<byte> footer)
            {
                _headerID = headerID;
                _detailID = detailID;
                _header = header;
                _detail = detail;
                _footer = footer;
            }

            public int HeaderID { get => _headerID; }
            public int DetailID { get => _detailID; }
            public Memory<byte> Header01 { get => _header.Slice(9, 8); }
            public Memory<byte> Header02 { get => _header.Slice(17, 14); }
            public Memory<byte> Header03 { get => _header.Slice(24, 10); }
            public Memory<byte> Header04 { get => _header.Slice(28, 4); }
            public Memory<byte> Header05 { get => _header.Slice(32, 6); }
            public Memory<byte> Header06 { get => _header.Slice(38, 6); }
            public Memory<byte> Header07 { get => _header.Slice(44, 12); }
            public Memory<byte> Data { get => _detail[..DETAIL_LEN]; }
            public Memory<byte> Footer01 { get => _footer.Slice(0, 10); }
            public Memory<byte> Footer02 { get => _footer.Slice(10, 18); }
            public Memory<byte> Footer03 { get => _footer.Slice(28, 4); }
            public Memory<byte> Footer04 { get => _footer.Slice(32, 6); }
            public Memory<byte> Footer05 { get => _footer.Slice(38, 16); }
            public Memory<byte> Footer06 { get => _footer.Slice(54, 10); }
            public Memory<byte> Footer07 { get => _footer.Slice(64, 6); }
            public Memory<byte> Footer08 { get => _footer.Slice(70, 8); }
        }
    }
}