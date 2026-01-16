using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Web.Models
{
    public class AnswerModel
    {
        public string description { get; set; }
        public int editorId { get; set; }
        public int isTrue { get; set; }
        public int questionNumber { get; set; }
    }
}