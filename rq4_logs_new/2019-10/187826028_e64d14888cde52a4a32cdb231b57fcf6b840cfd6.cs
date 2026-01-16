using System.Collections.Generic;
using System.Linq;
using HtmlAgilityPack;
using Module.Hsnr.Timetable.Data;

namespace Module.Hsnr.Timetable.Parser
{
    public class SubjectParser : ISubjectParser
    {
        public IEnumerable<Subject> Parse((IEnumerable<HtmlNode> Rows, TimetableTime[] Times) value)
        {
            foreach (var row in value.Rows)
            {
                var currentTime = 0;
                foreach (var childNode in row.ChildNodes)
                {
                    var firstChildOfChild = childNode.ChildNodes.FirstOrDefault(x => x.OriginalName == "a");
                    
                    if (firstChildOfChild != default)
                    {
                        var lecturerAndRoom = firstChildOfChild
                            .ChildNodes[2]
                            .InnerText.Trim()
                            .Replace(" &nbsp ", " ")
                            .Split(' ');

                        yield return new Subject()
                        {
                            Start = value.Times[currentTime].Start,
                            End = value.Times[currentTime + childNode.GetAttributeValue("colspan", 1) - 1].End,
                            Name = firstChildOfChild.ChildNodes[0].InnerHtml.Trim(),
                            Lecturer = lecturerAndRoom[0],
                            Room = lecturerAndRoom[1],
                        };
                    }

                    if (!childNode.Id.Contains("WT"))
                    {
                        currentTime += childNode.GetAttributeValue("colspan", 1);
                    }
                }
            }
        }
    }
}