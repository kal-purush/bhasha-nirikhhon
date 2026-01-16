using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise6
    {
        public List<string> FindStudentContainsName(string x)
        {
            using var dbContext = new DataContextFactory().CreateDbContext(new string[0]);

            return dbContext.Student.Where(s => s.Name.Contains(x))
                                    .Select(s => s.Name)
                                    .ToList();
        }
    }
}