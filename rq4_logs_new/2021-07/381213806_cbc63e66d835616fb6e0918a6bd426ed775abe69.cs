using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise8
    {
        public List<string> ScoreLowerThanFiveStudents()
        {
            using var dbContext = new DataContextFactory().CreateDbContext(new string[0]);
            return dbContext.Student.Where(s => s.Score < 5)
                                    .Select(s => s.Name)
                                    .ToList();
        }
    }
}