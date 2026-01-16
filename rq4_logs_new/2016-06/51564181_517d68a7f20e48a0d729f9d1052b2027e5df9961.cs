using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace AES.WebApplication.Models
{
    public class Questionnaire
    {
        public Questionnaire()
        {
            questionList = new List<Question>();
        }
        public int Id { get; set; }
        public List<Question> questionList { get; set; }
    }

    public class Question
    {
        public Question()
        {
            answerList = new List<Answer>();
        }

        public string Title { get; set; }
        public List<Answer> answerList { get; set; }
        public string correctAnswer { get; set; }
        public string selectedAnswer { get; set; }
        public string type { get; set; }

    }

    public class Answer
    {
        public Answer(string a, string s)
        {
            answerText = a;
            answerLetter = s;
        }
        public string answerText { get; set; }
        public string answerLetter { get; set; }
    }
}