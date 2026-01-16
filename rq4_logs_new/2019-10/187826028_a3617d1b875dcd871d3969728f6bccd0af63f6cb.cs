using System.Collections.Generic;
using HtmlAgilityPack;
using Module.Hsnr.Timetable.Data;

namespace Module.Hsnr.Factories
{
    public class SubjectFactory
    {
        public Subject Create(HtmlNode node, int start, int end)
        {
            var lecturerAndRoom = node.ChildNodes[2].InnerText.Trim()
                .Replace(" &nbsp ", " ").Split(' ');
            return new Subject()
            {
                Start = start,
                End = end,
                Name = node.ChildNodes[0].InnerText.Trim(),
                Lecturer = lecturerAndRoom[0],
                Room = lecturerAndRoom[1]
            };
        }
    }
}