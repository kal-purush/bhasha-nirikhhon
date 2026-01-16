using AutoFixture;
using OfficePerformanceReview.Domain.Common.ValueObjects;
using OfficePerformanceReview.Domain.PerformanceReview.Entities;
using OfficePerformanceReview.Domain.PerformanceReview.Enums;
using OfficePerformanceReview.Domain.PerformanceReview.Root;
using OfficeReview.Domain.Questions.Enum;
using System.Linq;
namespace OfficePerformanceReview.Tests
{

    public class PerformanceReviewTests : TestBase
    {
        private NameValue _completedBy = null!;
        private NameValue _appraisedName = null!;
        private DateTime _reviewDate;
        private int _performanceOverviewId;
        private QuestionFeedback _question;
        private RatingScale _ratingScale;

        public override void ExtendSetup()
        {
            base.ExtendSetup();

            Fixture.Customize<NameValue>(_ => _
              .FromFactory(() => new NameValue(Faker.Random.Int(), Faker.Name.FullName()))
          );

            _completedBy = Fixture.Create<NameValue>();
            _appraisedName = Fixture.Create<NameValue>();
            _reviewDate = Faker.Date.Recent();
            _performanceOverviewId = Faker.Random.Int(1);



            _question = Fixture.Create<QuestionFeedback>();
            _ratingScale = Enumeration.GetRandomEnumValue<RatingScale>();

            Fixture.Customize<Objective>(_ => _
           .FromFactory(() => new Objective(Faker.Lorem.Sentence(),
           Faker.Random.String2(10),
           new DateRange(Faker.Date.RecentDateOnly(), Faker.Date.FutureDateOnly()),
            Enumeration.GetRandomEnumValue<ObjectiveStatus>()))
       );
        }
        private PerformanceReview _SetPerformanceReview()
        {

            return new Faker<PerformanceReview>()
                .CustomInstantiator(f => new PerformanceReview(_completedBy,
               _appraisedName,
                _reviewDate, _performanceOverviewId))
                .RuleFor(pr => pr.Feedbacks, f => new Feedback(FeedbackStatus.Pending));
        }


        [Test]
        public void Constructor_ShouldInitializeProperties()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();

            // Act

            // Assert
            performanceReview.CompletedBy.Should().Be(_completedBy);
            performanceReview.AppraisedName.Should().Be(_appraisedName);
            performanceReview.ReviewDate.Should().Be(_reviewDate);
            performanceReview.PerformanceOverviewId.Should().Be(_performanceOverviewId);
            performanceReview.FeedbackStatus.Should().Be(FeedbackStatus.Pending);
        }

        [Test]
        public void SetPerformanceReview_ShouldUpdateProperties()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();
            var newAppraisedName = Fixture.Create<NameValue>();

            var newReviewDate = Faker.Date.Recent();

            // Act
            performanceReview.SetPerformanceReview(newAppraisedName, newReviewDate);

            // Assert
            performanceReview.AppraisedName.Should().Be(newAppraisedName);
            performanceReview.ReviewDate.Should().Be(newReviewDate);
        }

        [Test]
        public void AddPeerEvaluation_ShouldAddPeerEvaluation()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();

            var completedById = Faker.Random.Int();
            var completedBy = Faker.Name.FullName();
            var deadline = DateOnly.FromDateTime(DateTime.Now.AddDays(7));

            // Act
            performanceReview.AddPeerEvaluation(completedById, completedBy, deadline);

            // Assert
            performanceReview.Evaluators.Should()
                .ContainSingle(e => e.CompletedBy.Id == completedById && e.CompletedBy.Name == completedBy
                && e.DeadLine == deadline);
        }

        [Test]
        public void SetPeerEvaluation_ShouldSetPeerEvaluation()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();

            var completedById = Faker.Random.Int();
            var completedBy = Faker.Name.FullName();
            var deadline = DateOnly.FromDateTime(DateTime.Now.AddDays(7));
            performanceReview.AddPeerEvaluation(completedById, completedBy, deadline);

            var peerEvaluationGuid = performanceReview.Evaluators[0].PeerEvaluationGuid;
            var newCompletedBy = Faker.Name.FullName();
            var newcompletedById = Faker.Random.Int();
            // Act
            performanceReview.SetPeerEvaluation(peerEvaluationGuid, newcompletedById, newCompletedBy, deadline);

            // Assert
            var evaluator = performanceReview.Evaluators[0];
            evaluator.CompletedBy.Name.Should().Be(newCompletedBy);
            evaluator.CompletedBy.Id.Should().Be(newcompletedById);

        }

        [Test]
        public void SetFeedback_ShouldSetFeedback()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();


            var completedById = Faker.Random.Int();
            var completedBy = Faker.Name.FullName();
            var deadline = DateOnly.FromDateTime(DateTime.Now.AddDays(7));

            performanceReview.AddPeerEvaluation(completedById, completedBy, deadline);
            // Act
            performanceReview.SetFeedback(performanceReview.Evaluators.First().PeerEvaluationGuid, new List<QuestionFeedback> { _question });

            // Assert
            performanceReview.Evaluators[0].Feedbacks.Should().ContainSingle(f => f.QuestionId == _question.QuestionId);
        }

        [Test]
        public void AddBehaviorMetricByEmployee_ShouldAddMetric()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();


            var employeeRemarks = Fixture.Create<string>();

            // Act
            performanceReview.AddBehaviorMetricByEmployee(_question, _ratingScale, employeeRemarks);

            // Assert
            performanceReview.Feedbacks.BehaviorMetrics.Should().ContainSingle(m => m.Metric == _question
            && m.RevieweeRating == _ratingScale && m.EmployeeRemarks == employeeRemarks);
        }

        [Test]
        public void SetBehaviorMetricByEmployee_ShouldUpdateBehaviorMetric()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();

            var employeeRemarks = Fixture.Create<string>();
            performanceReview.AddBehaviorMetricByEmployee(_question, _ratingScale, employeeRemarks);
            var metricGUID = performanceReview.Feedbacks.BehaviorMetrics.First().MetricGUID;

            var newRatingScale = Enumeration.GetRandomEnumValue<RatingScale>();
            var newEmployeeRemarks = Fixture.Create<string>();

            // Act
            performanceReview.SetBehaviorMetricByEmployee(metricGUID, newRatingScale, newEmployeeRemarks);

            // Assert
            var metric = performanceReview.Feedbacks.BehaviorMetrics.First();
            metric.RevieweeRating.Should().Be(newRatingScale);
            metric.EmployeeRemarks.Should().Be(newEmployeeRemarks);
        }

        [Test]
        public void SetEmployee_ShouldUpdateEmployeeFeedback()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();
            var employeeComment = Fixture.Create<string>();
            var feedbackStatus = Fixture.Create<FeedbackStatus>();

            // Act
            performanceReview.SetEmployee(employeeComment, feedbackStatus);

            // Assert
            performanceReview.Feedbacks.EmployeeComment.Should().Be(employeeComment);
            performanceReview.Feedbacks.EmployeeFeedbackStatus.Should().Be(feedbackStatus);
        }


        [Test]
        public void SetBehaviorMetricByManager_ShouldUpdateBehaviorMetric()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();


            var employeeRemarks = Fixture.Create<string>();
            performanceReview.AddBehaviorMetricByEmployee(_question, _ratingScale, employeeRemarks);
            var metricGUID = performanceReview.Feedbacks.BehaviorMetrics.First().MetricGUID;

            var newRatingScale = Enumeration.GetRandomEnumValue<RatingScale>();
            var managerRemarks = Fixture.Create<string>();

            // Act
            performanceReview.SetBehaviorMetricByManager(metricGUID, newRatingScale, managerRemarks);

            // Assert
            var metric = performanceReview.Feedbacks.BehaviorMetrics.First();
            metric.ManagerRating.Should().Be(newRatingScale);
            metric.ManagerRemarks.Should().Be(managerRemarks);
        }

        [Test]
        public void SetManager_ShouldUpdateManagerFeedback()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();

            var feedbackStatus = Fixture.Create<FeedbackStatus>();
            var managerComment = Fixture.Create<string>();
            var overallRating = Fixture.Create<OverallRating>();

            // Act
            performanceReview.SetManager(feedbackStatus, managerComment, overallRating);

            // Assert
            performanceReview.Feedbacks.ManagerFeedbackStatus.Should().Be(feedbackStatus);
            performanceReview.Feedbacks.ReviewerComment.Should().Be(managerComment);
            performanceReview.Feedbacks.PotentialLevel.Should().Be(overallRating);
        }

        [Test]
        public void AddObjective_ShouldAddObjective()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();

            var objective = Fixture.Create<Objective>();

            // Act
            performanceReview.AddObjective(new List<Objective> { objective });

            // Assert
            performanceReview.Objectives.Should().ContainSingle(o => o.Description == objective.Description
            && o.ActionPlan == objective.ActionPlan
            && o.Timeline == objective.Timeline
            && o.ProgressStatus == objective.ProgressStatus);
        }

        [Test]
        public void SetObjective_ShouldUpdateObjective()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();

            var objectives = Fixture.CreateMany<Objective>(3).ToList();
            performanceReview.AddObjective(objectives);

            // Act
            var newobjectives = Fixture.Create<Objective>();

            performanceReview.SetObjective(newobjectives.Description, newobjectives.ActionPlan, newobjectives.Timeline,
                    newobjectives.ProgressStatus, objectives[0].ObjectiveGuid);       

            // Assert
          
                var existingObjective = performanceReview.Objectives[0];
                existingObjective.Description.Should().Be(newobjectives.Description);
                existingObjective.ActionPlan.Should().Be(newobjectives.ActionPlan);
                existingObjective.Timeline.Should().Be(newobjectives.Timeline);
                existingObjective.ProgressStatus.Should().Be(newobjectives.ProgressStatus);
            

        }

        [Test]
        public void RemoveObjective_ShouldRemoveObjective()
        {
            // Arrange
            var performanceReview = _SetPerformanceReview();

            var objectives = Fixture.CreateMany<Objective>(3).ToList();
            performanceReview.AddObjective(objectives);
            var guidsToRemove = objectives.Take(2).Select(o => o.ObjectiveGuid).ToList();

            // Act
            performanceReview.RemoveObjective(guidsToRemove);

            // Assert
            performanceReview.Objectives.Should().OnlyContain(o => !guidsToRemove.Contains(o.ObjectiveGuid));
        }
    }
}