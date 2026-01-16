using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WorkingOut.Models
{
    public class AddedExerciseViewModel
    {
        public WorkoutExercise Exercise { get; set; }
        public int ExerciseIndex { get; set; }
        public string ExerciseName { get; set; }
        public List<Set> Sets { get; set; }
        public bool Delete { get; set; }
    }
}