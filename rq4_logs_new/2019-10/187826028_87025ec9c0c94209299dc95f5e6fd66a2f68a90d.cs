using System.Collections.Generic;
using HtmlAgilityPack;
using Module.Hsnr.Timetable.Data;

namespace Module.Hsnr.Timetable.Parser
{
    public class TimetableTimeParser : ITimetableTimeParser
    {
        public IEnumerable<TimetableTime> Parse(HtmlNode value)
        {
            var timings = new List<TimetableTime>();
            
            foreach (var child in value.ChildNodes)
            {
                if (string.IsNullOrWhiteSpace(child.InnerHtml))
                {
                    continue;
                }

                var splittedTimings = child.InnerText.Split('-');
                timings.Add(new TimetableTime(int.Parse(splittedTimings[0]), int.Parse(splittedTimings[1])));
            }

            return timings;
        }
    }

    public interface ITimetableTimeParser : IParser<HtmlNode, IEnumerable<TimetableTime>>
    {
    }
}