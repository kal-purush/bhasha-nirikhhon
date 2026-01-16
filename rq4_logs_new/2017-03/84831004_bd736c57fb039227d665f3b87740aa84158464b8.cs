using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace ExcelParser
{
    class FinalMockExamExcelConverter
    {
        public static XmlElement Convert(XmlDocument xml, Excel<TestExcelColumn, TestExcelColumnType> finalMockExamExcel)
        {
            var chapterNode = xml.CreateElement("chapter");
            chapterNode.SetAttribute("display_name", "Final Mock Examination");
            chapterNode.SetAttribute("url_name", CourseConverterHelper.getGuid("FinalMockExamChapterNode", CourseTypes.Topic));
            chapterNode.SetAttribute("cfa_type", "final_mock_exam");
            chapterNode.SetAttribute("cfa_short_name", "Final Mock Exam");

            var amRows = new List<List<IExcelColumn<TestExcelColumnType>>>();
            var pmRows = new List<List<IExcelColumn<TestExcelColumnType>>>();
            string amFcmNumber = "";
            string pmFcmNumber = "";

            foreach (var row in finalMockExamExcel.Rows)
            {
                var fcmNumber = row.First(tn => tn.Type == TestExcelColumnType.FcmNumber).Value;
                if (fcmNumber.Contains("_AM"))
                {
                    amRows.Add(row);
                    amFcmNumber = fcmNumber;
                }
                else if (fcmNumber.Contains("_PM")) {
                    pmRows.Add(row);
                    pmFcmNumber = fcmNumber;
                }
            }

            var amSequentialNode = GetMockExamSequantialNode(xml, "AM", amFcmNumber, amRows);
            var pmSequentialNode = GetMockExamSequantialNode(xml, "PM", pmFcmNumber, pmRows);

            chapterNode.AppendChild(amSequentialNode);
            chapterNode.AppendChild(pmSequentialNode);


            return chapterNode;
        }


        private static XmlNode GetMockExamSequantialNode(XmlDocument xml, string displayName, string fcmNumber, List<List<IExcelColumn<TestExcelColumnType>>> rows)
        {
            var pdfAnswers = rows.First().FirstOrDefault(tn => tn.Type == TestExcelColumnType.PdfAnswers).Value;
            var pdfQuestions = rows.First().FirstOrDefault(tn => tn.Type == TestExcelColumnType.PdfQuestions).Value;
            var sequentialNode = xml.CreateElement("sequential");
            sequentialNode.SetAttribute("display_name", displayName);
            sequentialNode.SetAttribute("url_name", CourseConverterHelper.getGuid(String.Format("final-mock-sequential-{0}-{1}", displayName, fcmNumber), CourseTypes.Mock));
            sequentialNode.SetAttribute("taxon_id", fcmNumber);
            sequentialNode.SetAttribute("pdf_answers", pdfAnswers);
            sequentialNode.SetAttribute("pdf_questions", pdfQuestions);

            var topicNameGroup = rows.GroupBy(r=> r.First(tn => tn.Type == TestExcelColumnType.TopicName).Value);

            foreach (var topic in topicNameGroup)
            {
                string topicName = topic.Key;
                string topicTaxonId = topic.First().FirstOrDefault(c => c.Type == TestExcelColumnType.TopicTaxonId).Value;
                var verticalNode = xml.CreateElement("vertical");
                verticalNode.SetAttribute("display_name", topicName );
                verticalNode.SetAttribute("study_session_test_id", "");
                verticalNode.SetAttribute("taxon_id", topicTaxonId);
                verticalNode.SetAttribute("url_name", CourseConverterHelper.getGuid(String.Format("final-mock-vertical-{0}-{1}", displayName, topicName), CourseTypes.Mock));

                sequentialNode.AppendChild(verticalNode);

                var problemBuilderNode = ProblemBuilderNodeGenerator.Generate(xml, topic, new ProblemBuilderNodeSettings
                {
                    DisplayName = String.Format("Final Mock exam - {0} questions", displayName),
                    UrlName = CourseConverterHelper.getGuid(String.Format("final-mock-progress-test-{0}-{1}", displayName, topicName), CourseTypes.Mock),
                    ProblemBuilderNodeElement = "problem-builder-mock-exam",
                    PbMcqNodeElement = "pb-mcq-mock-exam",
                    PbChoiceBlockElement = "pb-choice-mock-exam",
                    PbTipBlockElement = "pb-tip-mock-exam"
                });

                verticalNode.AppendChild(problemBuilderNode);
            }

            return sequentialNode;
        }
    }
}