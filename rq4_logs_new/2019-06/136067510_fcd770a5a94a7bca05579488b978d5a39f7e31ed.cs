using GalaxyZooTouchTable.Models;
using GalaxyZooTouchTable.Tests.Mock;
using GalaxyZooTouchTable.ViewModels;
using System.Collections.Generic;
using Xunit;

namespace GalaxyZooTouchTable.Tests.ViewModels
{
    public class ClassificationSummaryViewModelTests
    {
        ClassificationSummaryViewModel _viewModel { get; set; }

        public ClassificationSummaryViewModelTests()
        {
            NotificationsViewModel notifications = new NotificationsViewModel(new BlueUser());
            _viewModel = new ClassificationSummaryViewModel(notifications);
        }

        [Fact]
        public void ShouldProcessAClassification()
        {
            TableSubject subject = PanoptesServiceMockData.TableSubject();
            ClassificationCounts counts = GraphQLServiceMockData.ClassificationCounts();
            List<AnswerButton> answers = PanoptesServiceMockData.AnswerButtons();
            AnswerButton selectedAnswer = answers[0];

            _viewModel.ProcessNewClassification(subject.Location, counts, answers, selectedAnswer);

            Assert.NotNull(_viewModel.ClassificationSummary.SelectedAnswer);
            Assert.NotNull(_viewModel.ClassificationSummary.CurrentAnswers);
            Assert.NotNull(_viewModel.ClassificationSummary.SubjectLocation);
            Assert.NotNull(_viewModel.ClassificationSummary.SummaryString);
            Assert.True(_viewModel.ClassificationSummary.TotalVotes > 0);
        }
    }
}