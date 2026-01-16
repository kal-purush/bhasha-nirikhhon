using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise14
    {
        public List<string> OrderStudentsByName(string className)
        {
            using var dbContext = new DataContextFactory().CreateDbContext(new string[0]);

            var innerJoin = from student in dbContext.Student
                            join @class in dbContext.Class
                            on student.Class.Id equals @class.Id
                            select new { student, @class };

            return innerJoin.AsEnumerable()
                            .Where(s => s.@class.Name.Equals(className))
                            .OrderBy(s => s.student.Name.Split(" ").Last())
                            .ThenBy(s => s.student.Name.Split(" ").First())
                            .Select(s => s.student.Name)
                            .ToList();
        }
    }
}