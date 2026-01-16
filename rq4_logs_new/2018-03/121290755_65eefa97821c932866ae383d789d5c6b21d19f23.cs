using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace ReactTesting.Models
{
    public class AddQuestionVM
    {
        [Required]
        public string Category { get; set; }
        [Required, Display(Name = "Correct answer")]
        public string CorrectAnswer { get; set; }
        [Required]
        public string Difficulty { get; set; }
        [Required, Display(Name = "Incorrect answers")]
        public List<string> IncorrectAnswers { get; set; }
        [Required]
        public string Question { get; set; }
        //public string Type { get; set; }
    }

    public enum Difficulty
    {
        Easy,
        Medium,
        Hard
    }
}