using System.Collections.Generic;
using WorkOutous.Models;

namespace WorkOutous.Services
{
    public interface ISearchExercisesService
    {
         List<Exercises> GetExercises();
    }
}