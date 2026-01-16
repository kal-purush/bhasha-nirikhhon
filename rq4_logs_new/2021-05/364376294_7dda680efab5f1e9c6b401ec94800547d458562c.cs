using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Components;
using SurveyWebsite.DataAccessLibrary.Models;

namespace SurveyWebsite.DataAccessLibrary.Models
{
    public class QuestionListBase : ComponentBase
    {
        public static IEnumerable<Question> Questions { get; set; }

        protected override Task OnInitializedAsync()
        {
            LoadQuestions();
            return base.OnInitializedAsync();
        }
        
        public void LoadQuestions()
        {
            Question q1 = new Question
            {
                ID = 1,
                Questions = "what is your favourite color?"
            };
            Question q2 = new Question
            {
                ID = 2,
                Questions = "what is your favourite drink?"
            };
            Question q3 = new Question
            {
                ID = 3,
                Questions = "what is your favourite food?"
            };

            Questions = new List<Question> { q1, q2,q3};

        }
    }
}