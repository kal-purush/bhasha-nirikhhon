using System.IO;

namespace Grades
{
    internal interface IGradeTracker
    {
        void AddGrade(float grade);
        GradeStatististics ComputeStatistics();
        void WriteGrades(TextWriter destination);
        string Name { get; set; }

    }
}