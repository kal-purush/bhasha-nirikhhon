using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise5
    {
        public List<string> MaxScoreGroupByClass()
        {
            using var dbContext = new DataContextFactory().CreateDbContext(new string[0]);

            var innerJoin = from student in dbContext.Student
                            join @class in dbContext.Class
                            on student.Class.Id equals @class.Id
                            select new { student, @class };

            return innerJoin.AsEnumerable()
                            .GroupBy(s => s.@class.Name)
                            .Select(g => g.OrderByDescending(s => s.student.Score)
                            .First().student.Name)
                            .ToList();
        }
    }
}