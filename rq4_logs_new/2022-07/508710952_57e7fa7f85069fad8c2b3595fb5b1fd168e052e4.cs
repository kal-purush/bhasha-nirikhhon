using BenchmarkDotNet.Attributes;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace ImportingProcess
{
    [MarkdownExporterAttribute.GitHub]
    [ShortRunJob]
    [MemoryDiagnoser]
    public class PipeTest
    {
        const string INPUT_FILE = @"data01.txt";
        const int DETAIL_COUNT = 10;

        #region char count
        const int HEADER_LEN = 39;
        const int DETAIL_LEN = 10;
        const int FOOTER_LEN = 39;

        readonly int _footerOffset = HEADER_LEN + DETAIL_LEN * DETAIL_COUNT;
        readonly int _totalLength = HEADER_LEN + DETAIL_LEN * DETAIL_COUNT + FOOTER_LEN;
        #endregion

        #region byte count
        const int HEADER_BYTE_LEN = 9 + 30 * 2;
        const int DETAIL_BYTE_LEN = 10;
        const int FOOTER_BYTE_LEN = 39 * 2;

        readonly int _totalLengthByte = HEADER_BYTE_LEN + DETAIL_BYTE_LEN * DETAIL_COUNT + FOOTER_BYTE_LEN + 2;
        readonly int _footerOffsetByte = HEADER_BYTE_LEN + DETAIL_BYTE_LEN * DETAIL_COUNT;
        readonly int _footerEndByte = HEADER_BYTE_LEN + DETAIL_BYTE_LEN * DETAIL_COUNT + FOOTER_BYTE_LEN;
        #endregion

        readonly byte[] _crlf = Encoding.ASCII.GetBytes("\r\n");
        readonly byte[] _comma = Encoding.ASCII.GetBytes(",");

        readonly byte[] _input;
        readonly Encoding _enc;

        public PipeTest()
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            _input = Enumerable.Repeat(File.ReadAllBytes(INPUT_FILE), 1000).SelectMany(x => x).ToArray();
            _enc = Encoding.GetEncoding("shift-jis");
        }

        #region Baseline
        [Benchmark(Baseline = true)]
        public async Task UseStreamReaderAsync()
        {
            var input = new MemoryStream(_input);
            var output = new MemoryStream();
            var details = Import(input);
            await WriteFileAsync(output, details);
        }

        private IEnumerable<Row> Import(Stream st)
        {
            var lineNum = 0;
            var details = new List<Row>();
            using (var sr = new StreamReader(st, _enc))
            {
                while (!sr.EndOfStream)
                {
                    lineNum++;
                    var line = sr.ReadLine();
                    if (line == null || line.Length != _totalLength)
                        throw new ApplicationException($"Data length differs line:{lineNum}");

                    if (string.IsNullOrEmpty(line))
                        break;

                    var header = line[..HEADER_LEN];
                    var footer = line[_footerOffset.._totalLength];

                    var offset = HEADER_LEN;
                    for (int i = 0; i < DETAIL_COUNT; i++)
                    {
                        var offsetEnd = offset + DETAIL_LEN;
                        offset += DETAIL_LEN;
                        details.Add(new Row()
                        {
                            Header01 = header[9..13],
                            Header02 = header[13..20],
                            Header03 = header[20..25],
                            Header04 = header[25..27],
                            Header05 = header[27..30],
                            Header06 = header[30..33],
                            Header07 = header[33..39],
                            Data = line[offset..offsetEnd],
                            Footer01 = footer[0..5],
                            Footer02 = footer[5..14],
                            Footer03 = footer[14..16],
                            Footer04 = footer[16..19],
                            Footer05 = footer[19..27],
                            Footer06 = footer[27..32],
                            Footer07 = footer[32..35],
                            Footer08 = footer[35..39],
                        });
                    }
                }
            }
            return details;
        }

        private async Task WriteFileAsync(Stream output, IEnumerable<Row> details)
        {
            using (var sw = new StreamWriter(output, _enc))
            {
                foreach (var d in details)
                {
                    var line = $"{d.Data},{d.Header01}{d.Header02}{d.Header03}{d.Header04}{d.Header05}{d.Header06}{d.Header07},{d.Footer01}{d.Footer02}{d.Footer03}{d.Footer04}{d.Footer05}{d.Footer06}{d.Footer07}{d.Footer08}";
                    await sw.WriteLineAsync(line);
                }
            }
        }

        public class Row
        {
            public string Header01 { get; set; } = "";
            public string Header02 { get; set; } = "";
            public string Header03 { get; set; } = "";
            public string Header04 { get; set; } = "";
            public string Header05 { get; set; } = "";
            public string Header06 { get; set; } = "";
            public string Header07 { get; set; } = "";
            public string Data { get; set; } = "";
            public string Footer01 { get; set; } = "";
            public string Footer02 { get; set; } = "";
            public string Footer03 { get; set; } = "";
            public string Footer04 { get; set; } = "";
            public string Footer05 { get; set; } = "";
            public string Footer06 { get; set; } = "";
            public string Footer07 { get; set; } = "";
            public string Footer08 { get; set; } = "";
        }
        #endregion

        #region Pipe_SequencePosition
        [Benchmark]
        public async Task Pipe_MemoryAsync()
        {
            var input = new MemoryStream(_input);
            var output = new MemoryStream();

            var pipe = new Pipe();
            var writing = FillPipeAsync(input, pipe.Writer);
            var reading = ReadSequencePositionAsync(output, pipe.Reader);
            await Task.WhenAll(writing, reading);
        }

        public static async Task FillPipeAsync(Stream stream, PipeWriter writer)
        {
            while (true)
            {
                try
                {
                    var memory = writer.GetMemory();
                    int byteRead = await stream.ReadAsync(memory);
                    if (byteRead == 0)
                        break;
                    writer.Advance(byteRead);
                }
                catch
                {
                    break;
                }
                var result = await writer.FlushAsync();
                if (result.IsCompleted)
                    break;
            }
            writer.Complete();
        }

        private async Task ReadSequencePositionAsync(Stream output, PipeReader reader)
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;
                SequencePosition? position;
                do
                {
                    position = buffer.PositionOf(_crlf.Last());
                    if (position != null)
                    {
                        try
                        {
                            OutputReadOnlySequence(output, buffer.Slice(0, position.Value));
                        }
                        catch (Exception ex)
                        {
                            reader.Complete(ex);
                            return;
                        }
                        buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
                    }
                } while (position != null);
                reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted)
                    break;
            }
            reader.Complete();
        }

        private void OutputReadOnlySequence(Stream output, in ReadOnlySequence<byte> lineSegment)
        {
            var line = lineSegment.IsSingleSegment
                ? lineSegment.First
                : lineSegment.ToArray();

            var offset = HEADER_BYTE_LEN;
            for (int i = 0; i < DETAIL_COUNT; i++)
            {
                var r = new RowMemory(
                    line[..HEADER_BYTE_LEN],
                    line.Slice(offset, DETAIL_BYTE_LEN),
                    line.Slice(_footerOffsetByte, FOOTER_BYTE_LEN)
                );
                r.Export(output, _comma, _crlf);
                offset += DETAIL_BYTE_LEN;
            }
        }

        [Benchmark]
        public async Task Pipe_StructAsync()
        {
            var input = new MemoryStream(_input);
            var output = new MemoryStream();

            var pipe = new Pipe();
            var writing = FillPipeAsync(input, pipe.Writer);
            var reading = ReadSequencePositionStructAsync(output, pipe.Reader);
            await Task.WhenAll(writing, reading);
        }

        private async Task ReadSequencePositionStructAsync(Stream output, PipeReader reader)
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;
                SequencePosition? position;
                do
                {
                    position = buffer.PositionOf(_crlf.Last());
                    if (position != null)
                    {
                        try
                        {
                            OutputReadOnlySequenceStruct(output, buffer.Slice(0, position.Value));
                        }
                        catch (Exception ex)
                        {
                            reader.Complete(ex);
                            return;
                        }
                        buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
                    }
                } while (position != null);
                reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted)
                    break;
            }
            reader.Complete();
        }

        private void OutputReadOnlySequenceStruct(Stream output, in ReadOnlySequence<byte> lineSegment)
        {
            var line = lineSegment.IsSingleSegment
                ? lineSegment.First.Span
                : lineSegment.ToArray().AsSpan();

            var offset = HEADER_BYTE_LEN;
            for (int i = 0; i < DETAIL_COUNT; i++)
            {
                var r = new RowStruct(
                    line[..HEADER_BYTE_LEN],
                    line.Slice(offset, DETAIL_BYTE_LEN),
                    line.Slice(_footerOffsetByte, FOOTER_BYTE_LEN)
                );
                r.Export(output, _comma, _crlf);
                offset += DETAIL_BYTE_LEN;
            }
        }
        #endregion

        #region Row 
        internal class RowMemory
        {
            readonly ReadOnlyMemory<byte> _header;
            readonly ReadOnlyMemory<byte> _detail;
            readonly ReadOnlyMemory<byte> _footer;

            public RowMemory(ReadOnlyMemory<byte> header, ReadOnlyMemory<byte> detail, ReadOnlyMemory<byte> footer)
            {
                _header = header;
                _detail = detail;
                _footer = footer;
            }

            ReadOnlySpan<byte> Header01 => _header[9..17].Span;
            ReadOnlySpan<byte> Header02 => _header[17..31].Span;
            ReadOnlySpan<byte> Header03 => _header[31..41].Span;
            ReadOnlySpan<byte> Header04 => _header[41..45].Span;
            ReadOnlySpan<byte> Header05 => _header[45..51].Span;
            ReadOnlySpan<byte> Header06 => _header[51..57].Span;
            ReadOnlySpan<byte> Header07 => _header[57..69].Span;
            ReadOnlySpan<byte> Data => _detail[..DETAIL_BYTE_LEN].Span;
            ReadOnlySpan<byte> Footer01 => _footer[..10].Span;
            ReadOnlySpan<byte> Footer02 => _footer[10..28].Span;
            ReadOnlySpan<byte> Footer03 => _footer[28..32].Span;
            ReadOnlySpan<byte> Footer04 => _footer[32..38].Span;
            ReadOnlySpan<byte> Footer05 => _footer[38..54].Span;
            ReadOnlySpan<byte> Footer06 => _footer[54..64].Span;
            ReadOnlySpan<byte> Footer07 => _footer[64..70].Span;
            ReadOnlySpan<byte> Footer08 => _footer[70..78].Span;

            public void Export(Stream output, in ReadOnlySpan<byte> comma, in ReadOnlySpan<byte> crlf)
            {
                output.Write(Data);
                output.Write(comma);
                output.Write(Header01);
                output.Write(Header02);
                output.Write(Header03);
                output.Write(Header04);
                output.Write(Header05);
                output.Write(Header06);
                output.Write(Header07);
                output.Write(comma);
                output.Write(Footer01);
                output.Write(Footer02);
                output.Write(Footer03);
                output.Write(Footer04);
                output.Write(Footer05);
                output.Write(Footer06);
                output.Write(Footer07);
                output.Write(Footer08);
                output.Write(crlf);
            }
        }

        internal readonly ref struct RowStruct
        {
            readonly ReadOnlySpan<byte> _header;
            readonly ReadOnlySpan<byte> _detail;
            readonly ReadOnlySpan<byte> _footer;

            public RowStruct(ReadOnlySpan<byte> header, ReadOnlySpan<byte> detail, ReadOnlySpan<byte> footer)
            {
                _header = header;
                _detail = detail;
                _footer = footer;
            }

            ReadOnlySpan<byte> Header01 => _header[9..17];
            ReadOnlySpan<byte> Header02 => _header[17..31];
            ReadOnlySpan<byte> Header03 => _header[31..41];
            ReadOnlySpan<byte> Header04 => _header[41..45];
            ReadOnlySpan<byte> Header05 => _header[45..51];
            ReadOnlySpan<byte> Header06 => _header[51..57];
            ReadOnlySpan<byte> Header07 => _header[57..69];
            ReadOnlySpan<byte> Data => _detail[..DETAIL_BYTE_LEN];
            ReadOnlySpan<byte> Footer01 => _footer[..10];
            ReadOnlySpan<byte> Footer02 => _footer[10..28];
            ReadOnlySpan<byte> Footer03 => _footer[28..32];
            ReadOnlySpan<byte> Footer04 => _footer[32..38];
            ReadOnlySpan<byte> Footer05 => _footer[38..54];
            ReadOnlySpan<byte> Footer06 => _footer[54..64];
            ReadOnlySpan<byte> Footer07 => _footer[64..70];
            ReadOnlySpan<byte> Footer08 => _footer[70..78];

            public void Export(Stream output, in ReadOnlySpan<byte> comma, in ReadOnlySpan<byte> crlf)
            {
                output.Write(Data);
                output.Write(comma);
                output.Write(Header01);
                output.Write(Header02);
                output.Write(Header03);
                output.Write(Header04);
                output.Write(Header05);
                output.Write(Header06);
                output.Write(Header07);
                output.Write(comma);
                output.Write(Footer01);
                output.Write(Footer02);
                output.Write(Footer03);
                output.Write(Footer04);
                output.Write(Footer05);
                output.Write(Footer06);
                output.Write(Footer07);
                output.Write(Footer08);
                output.Write(crlf);
            }
        }
        #endregion

        #region Pipe_SequenceReader
        [Benchmark]
        public async Task Pipe_SeqReaderAsync()
        {
            var input = new MemoryStream(_input);
            var output = new MemoryStream();

            var pipe = new Pipe();
            var writing = FillPipeAsync(input, pipe.Writer);
            var reading = ReadPipeSequenceReaderAsync(output, pipe.Reader);
            await Task.WhenAll(writing, reading);
        }

        private async Task ReadPipeSequenceReaderAsync(Stream output, PipeReader reader)
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;
                ProcessSequenceReader(output, ref buffer);
                reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted)
                    break;
            }
            reader.Complete();
        }

        private void ProcessSequenceReader(Stream output, ref ReadOnlySequence<byte> buffer)
        {
            var sequenceReader = new SequenceReader<byte>(buffer);
            while (!sequenceReader.End)
            {
                while (sequenceReader.TryReadTo(out ReadOnlySpan<byte> line, _crlf))
                    OutputReadOnlySpan(output, line);

                buffer = buffer.Slice(sequenceReader.Position);
                sequenceReader.Advance(buffer.Length);
            }
        }

        private void OutputReadOnlySpan(Stream output, in ReadOnlySpan<byte> line)
        {
            var offset = HEADER_BYTE_LEN;
            var offsetEnd = HEADER_BYTE_LEN + DETAIL_BYTE_LEN;
            for (int i = 0; i < DETAIL_COUNT; i++)
            {
                var r = new RowStruct(
                    line[..HEADER_BYTE_LEN],
                    line[offset..offsetEnd],
                    line[_footerOffsetByte.._footerEndByte]
                );
                r.Export(output, _comma, _crlf);
                offset += DETAIL_BYTE_LEN;
                offsetEnd += DETAIL_BYTE_LEN;
            }
        }
        #endregion

        #region StreamRead
        [Benchmark]
        public Task StreamRead()
        {
            var input = new MemoryStream(_input);
            var output = new MemoryStream();
            Convert(input, output);
            return Task.CompletedTask;
        }

        private void Convert(Stream input, Stream output)
        {
            Span<byte> buffer = stackalloc byte[_totalLengthByte];
            for (; ; )
            {
                var len = input.Read(buffer);
                if (len == 0)
                    break;

                if (!buffer.EndsWith(_crlf))
                    throw new ApplicationException("\r not found");

                var offset = HEADER_BYTE_LEN;
                var offsetEnd = HEADER_BYTE_LEN + DETAIL_BYTE_LEN;
                for (int i = 0; i < DETAIL_COUNT; i++)
                {
                    var r = new RowStruct(
                        buffer[..HEADER_BYTE_LEN],
                        buffer[offset..offsetEnd],
                        buffer[_footerOffsetByte.._footerEndByte]
                    );
                    r.Export(output, _comma, _crlf);
                    offset += DETAIL_BYTE_LEN;
                    offsetEnd += DETAIL_BYTE_LEN;
                }
            }
        }
        #endregion
    }
}