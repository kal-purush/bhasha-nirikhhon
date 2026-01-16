using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise2
    {
        public List<string> GetStudentsByYear(int year)
        {
            using var dbContext = new DataContextFactory().CreateDbContext(new string[0]);
            return dbContext.Student.Where(s => s.Birthday.Year == year)
                                    .Select(s => s.Name)
                                    .ToList();
        }

    }
}