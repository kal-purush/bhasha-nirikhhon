using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise.Exercises
{
    public class Exercise1
    {
        public double GetScoreById(int studentId)
        {
            using var context = new DataContextFactory().CreateDbContext(new string[0]);
            return context.Student.Where(s => s.Id == studentId)
                                  .Select(s => s.Score)
                                  .First()
                                  .Value;
        }
    }
}