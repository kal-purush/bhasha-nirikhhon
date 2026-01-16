using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise11
    {
        public string ClassHasHighestScore()
        {
            using var dbContext = new DataContextFactory().CreateDbContext(new string[0]);
            
            var innerJoin = from student in dbContext.Student
                            join @class in dbContext.Class
                            on student.Class.Id equals @class.Id
                            select new { student, @class };
            return innerJoin.AsEnumerable()
                            .GroupBy(student => student.@class.Name)
                            .Select(group => new { className = group.Key, score = group.Average(s => s.student.Score) })
                            .OrderByDescending(dict => dict.score)
                            .First().className;
        }
    }
}