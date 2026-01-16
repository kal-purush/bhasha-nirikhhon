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
    public class BaseLine
    {
        const int HEADER_LEN = 39;
        const int DETAIL_LEN = 10;
        const int DETAIL_COUNT = 10;
        const int FOOTER_LEN = 39;

        readonly int _footerOffset = HEADER_LEN + DETAIL_LEN * DETAIL_COUNT;
        readonly int _totalLength = HEADER_LEN + DETAIL_LEN * DETAIL_COUNT + FOOTER_LEN;

        readonly Encoding _enc;
        readonly byte[] _input;

        public BaseLine(byte[] input)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            _enc = Encoding.GetEncoding("shift-jis");
            _input = input;
        }

        public async Task RunAsync()
        {
            var details = Import();
            await WriteFileAsync(details);
            //Console.WriteLine(JsonConvert.SerializeObject(details));
        }

        private IEnumerable<Row> Import()
        {
            var details = new List<Row>();
            var lineNum = 1;
            using (var sr = new StreamReader(new MemoryStream(_input), _enc))
            {
                while (!sr.EndOfStream)
                {
                    var line = sr.ReadLine();
                    if (line.Length != _totalLength)
                        throw new ApplicationException($"Data length differs line:{lineNum}");

                    if (string.IsNullOrEmpty(line))
                        break;

                    var header = line.Substring(0, HEADER_LEN);
                    var footer = line.Substring(_footerOffset, FOOTER_LEN);

                    var offset = HEADER_LEN;
                    for (int i = 0; i < DETAIL_COUNT; i++)
                    {
                        var detail = line.Substring(offset, DETAIL_LEN);
                        offset += DETAIL_LEN;
                        details.Add(Convert(i + 1, header, detail, footer));
                    }
                }
            }
            return details;
        }

        private Row Convert(int detailID, string header, string detail, string footer)
        {
            if (!int.TryParse(header.AsSpan(0, 9), out var headerID))
                throw new ApplicationException("Could not be converted to int.");

            return new Row()
            {
                HeaderID = headerID,
                DetailID = detailID,
                Header01 = header.Substring(9, 4).TrimEnd(),
                Header02 = header.Substring(13, 7).TrimEnd(),
                Header03 = header.Substring(20, 5).TrimEnd(),
                Header04 = header.Substring(25, 2).TrimEnd(),
                Header05 = header.Substring(27, 3).TrimEnd(),
                Header06 = header.Substring(30, 3).TrimEnd(),
                Header07 = header.Substring(33, 6).TrimEnd(),
                Data = detail,
                Footer01 = footer.Substring(0, 5).TrimEnd(),
                Footer02 = footer.Substring(5, 9).TrimEnd(),
                Footer03 = footer.Substring(14, 2).TrimEnd(),
                Footer04 = footer.Substring(16, 3).TrimEnd(),
                Footer05 = footer.Substring(19, 8).TrimEnd(),
                Footer06 = footer.Substring(27, 5).TrimEnd(),
                Footer07 = footer.Substring(32, 3).TrimEnd(),
                Footer08 = footer.Substring(35, 4).TrimEnd(),
            };
        }

        private async Task WriteFileAsync(IEnumerable<Row> details)
        {
            using (var sw = new StreamWriter(new MemoryStream(), _enc))
            {
                foreach (var d in details)
                {
                    var line = $"{d.HeaderID},{d.DetailID:000},{d.Data},{d.Header01}{d.Header02}{d.Header03}{d.Header04}{d.Header05}{d.Header06}{d.Header07}{d.Footer01}{d.Footer02}{d.Footer03}{d.Footer04}{d.Footer05}{d.Footer06}{d.Footer07}{d.Footer08}";
                    await sw.WriteLineAsync(line);
                }
            }
        }
    }
}