using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace ScoreBoard
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                var rj = new ReadJson();
                var studentsJsonPath = "Students";
                var scoresJsonPath = "StudentsScore";

                var students = await rj.ReadAsync<Student>(studentsJsonPath);
                var studentScores = await rj.ReadAsync<StudentScore>(scoresJsonPath);

                var sop = new ScoreOperator(students, studentScores);
                sop.calculateAveg();
                sop.sortScores();
                var sorted = sop.takeFirst(3);

                foreach (var s in sorted)
                    Console.WriteLine(s.ToString());
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An unexpected error occurred: {ex.Message}");
            }
        }
    }
}