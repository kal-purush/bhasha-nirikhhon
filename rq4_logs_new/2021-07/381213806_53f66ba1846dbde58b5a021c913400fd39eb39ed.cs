using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise13
    {
        public bool CheckStudent(int year, double score)
        {
            using var dbContext = new DataContextFactory().CreateDbContext(new string[0]);
            return dbContext.Student
                            .Any(s => s.Class.Name.Equals("Class A")
                            && s.Birthday.Year == year
                            && s.Score >= score);
        }
    }
}