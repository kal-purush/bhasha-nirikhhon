using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace ReactTesting.Models
{
    public class QuizCreateVM
    {
        [Required]
        public string QuizName { get; set; }
        public Quiz Quiz { get; set; }
    }
}