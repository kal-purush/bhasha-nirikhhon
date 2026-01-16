using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ghosen.Parsers.Candump
{
    public class CandumpParser
    {
        public static async Task<List<CandumpLine>> ParseStream(Stream s)
        {
            var ret = new List<CandumpLine>();

            var streamLength = s.Length;
            using (StreamReader sr = new StreamReader(s))
            {
                while (!sr.EndOfStream)
                {
                    var progress = ((double)sr.BaseStream.Position / (double)sr.BaseStream.Length) * 100.0d;
                    var currentLineStr = await sr.ReadLineAsync();
                    var currentLine = CandumpLine.Parse(currentLineStr);
                    Debug.WriteLine(currentLine);
                    ret.Add(currentLine);
                }
            }

            return ret;
        }

        public static List<CandumpLine> ParseLines(string[] lines, CandumpParserArbIdFilter filter = null)
        {
            var ret = new List<CandumpLine>();
            var pastProgress = 0;
            for (int i = 0; i < lines.Length; i++)
            {
                var curProgress = ((i * 100) / lines.Length);
                if (curProgress != pastProgress)
                {
                    pastProgress = curProgress;
                    //Debug.WriteLine(curProgress);
                }

                var currentLineStr = lines[i];
                var currentLine = CandumpLine.Parse(currentLineStr);
                if (currentLine.Message != null)
                {
                    if (filter != null)
                    {
                        if (filter.ShouldAcceptLine(currentLine))
                        {
                            ret.Add(currentLine);
                        }
                        else
                        {
                            continue;
                        }
                    }
                    else
                    {
                        // we have no filter, accept all
                        ret.Add(currentLine);
                    }
                }
            }

            return ret;
        }

        public static CAN.Message ParseCandumpMessage(string messageString)
        {
            var ret = new CAN.Message();

            // Parse id first
            var arbIdMatcher = Regex.Match(messageString, @" ([A-Fa-f0-9]{3})#");

            // We could extract the arb id
            if (arbIdMatcher.Groups.Count == 2)
            {
                // extract the arb id
                var arbIdString = arbIdMatcher.Groups[1].Value;
                // parse the arb id
                var candidateArbId = uint.Parse(arbIdString, System.Globalization.NumberStyles.HexNumber);
                ret.ArbId = candidateArbId;
            }
            else
            {
                return null;
            }


            // Now handle data (only 8 byte packets at the moment)
            var rawDataMatcher = Regex.Match(messageString, @"#([A-Fa-f0-9]*)");

            // We could extract the raw data
            if (rawDataMatcher.Groups.Count == 2)
            {
                // extract the raw data
                var rawDataString = rawDataMatcher.Groups[1].Value;
                // parse the raw data
                var candidateRawData = Utils.StringToByteArrayFastest(rawDataString);
                ret.RawData = candidateRawData;
            }

            return ret;
        }
    }
}