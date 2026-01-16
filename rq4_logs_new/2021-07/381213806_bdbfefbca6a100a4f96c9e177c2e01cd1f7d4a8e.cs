using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise12
    {
        public List<string> RandomFiveStudentByClass(string className)
        {
            using var dbContext = new DataContextFactory().CreateDbContext(new string[0]);
            return dbContext.Student
                            .Where(s => s.Class.Name.Equals(className))
                            .Select(s => s.Name)
                            .Take(5)
                            .ToList();
        }
    }
}