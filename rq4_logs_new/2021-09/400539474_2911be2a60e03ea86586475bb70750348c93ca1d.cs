using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace P1_ASP_WebApp.Models
{
    public class CreatedReview
    {

        public string Text { get; set; }

        public List<string> Tags { get; set; }

        //public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
        //{
        //    //var repo = validationContext.GetService(typeof(IRepository));
        //    //repo.
        //}


        public string Phone { get; set; }
    }
}