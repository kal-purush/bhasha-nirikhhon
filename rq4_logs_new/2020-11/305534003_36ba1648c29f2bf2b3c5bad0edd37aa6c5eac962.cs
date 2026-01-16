using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace _7024Movies.Models
{
    public class ActorReview
    {
        public int ActorReviewID { get; set; }

        [DisplayName("Score")]
        [Range(1, 5, ErrorMessage = "Please rate the actor/actess from 1=Best to 5=Worst")]
        [Required]
        public int AReviewScore { get; set; }

        [DisplayName("Review")]
        [Required]
        [DataType(DataType.MultilineText)]
        public string AReviewText { get; set; }

        public MovieReview MovieReview { get; set; }
        [DisplayName("Movie")]
        public int MovieReviewId { get; set; }

    }
}