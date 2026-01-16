using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace _7024Movies.Models
{
    public class MovieReviewOther
    {
            public int MovieReviewOtherId { get; set; }

            [DisplayName("Movie")]
            [Required]
            [DataType(DataType.Text)]
            public string MovieNameOther { get; set; }

            [DisplayName("User Reviews")]
            [Required]
            [DataType(DataType.MultilineText)]
            public string MovieReviewO { get; set; }
        }
}