using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise10
    {
        public List<int[]> CountScoreHigherThanEight()
        {
            using var dbContext = new DataContextFactory().CreateDbContext(new string[0]);

            var innerJoin = from student in dbContext.Student
                            join @class in dbContext.Class
                            on student.Class.Id equals @class.Id
                            select new { student, @class };

            return innerJoin.AsEnumerable()
                            .GroupBy(s => s.@class.Name)
                            .Select(group => new[] { group.Count(), group.Count(s => s.student.Score > 8.0) })
                            .ToList();
        }                   
    }
}