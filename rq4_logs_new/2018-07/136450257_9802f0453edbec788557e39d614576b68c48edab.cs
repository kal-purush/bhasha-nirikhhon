using System;
using NCS.DSS.Goal.ReferenceData;

namespace NCS.DSS.Goal.Models
{
    public interface IGoal
    {
        DateTime? DateGoalCaptured { get; set; }
        DateTime? DateGoalShouldBeCompletedBy { get; set; }
        DateTime? DateGoalAchieved { get; set; }
        string GoalSummary { get; set; }
        GoalType? GoalType { get; set; }
        GoalStatus? GoalStatus { get; set; }
        DateTime? LastModifiedDate { get; set; }
        Guid? LastModifiedBy { get; set; }

        void SetDefaultValues();
    }
}