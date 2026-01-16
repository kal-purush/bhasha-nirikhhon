using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise3
    {
        public List<string> GetStudentsByClass(string className)
        {
            using var dbContext = new DataContextFactory().CreateDbContext(new string[0]);
            
            return dbContext.Student
                            .Where(s => s.Class.Name.Equals(className))
                            .Select(s => s.Name)
                            .ToList();
        }
    }
}