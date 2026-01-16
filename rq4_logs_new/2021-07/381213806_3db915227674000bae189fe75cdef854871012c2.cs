using EFCoreExercise.Exercises;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise
{
    class test
    {
        static void Main(String[] args)
        {
            var a = new Exercise5().MaxScoreGroupByClass();
            foreach (var item in a)
            {
                Console.WriteLine(item);
            }
        }
    }
}