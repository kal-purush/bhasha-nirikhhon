using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Windows;

namespace GalaxyZooTouchTable.Models
{
    public class ClassificationSummary
    {
        public List<AnswerButton> CurrentAnswers { get; private set; }
        public AnswerButton SelectedAnswer { get; private set; }
        public string SubjectLocation { get; private set; }
        public string SummaryString { get; private set; }
        public int TotalVotes { get; private set; }
        static Random random = new Random();

        public ClassificationSummary(string subjectLocation, ClassificationCounts counts, List<AnswerButton> currentAnswers, AnswerButton selectedAnswer)
        {
            CurrentAnswers = ParseAnswerCounts(currentAnswers, counts);
            SelectedAnswer = selectedAnswer;
            SubjectLocation = subjectLocation;
            SummaryString = SelectSummaryString();
            TotalVotes = counts.Total;
        }

        List<AnswerButton> ParseAnswerCounts(List<AnswerButton> answers, ClassificationCounts counts)
        {
            foreach (AnswerButton answer in answers)
            {
                switch (answer.Label)
                {
                    case "Smooth":
                        answer.AnswerCount = counts.Smooth;
                        break;
                    case "Features":
                        answer.AnswerCount = counts.Features;
                        break;
                    default:
                        answer.AnswerCount = counts.Star;
                        break;
                }
            }
            return answers;
        }

        string SelectSummaryString()
        {
            ObservableCollection<string> summaryStrings = Application.Current.FindResource("SummaryStrings") as ObservableCollection<string>;
            int index = random.Next(summaryStrings.Count);
            return summaryStrings[index];
        }
    }
}