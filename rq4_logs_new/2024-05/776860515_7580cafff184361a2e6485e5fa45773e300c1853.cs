using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System;

namespace GiLuFlixBack.Models.ReviewDTO
{
    public class ReviewResponse
    {
        [Key]
        public int ReviewId { get; set; }

        public int UserId { get; set; } 
        public string Name { get; set; } 
        public int ItemId { get; set; }
        public int Rating { get; set; }
        public string ReviewText { get; set; }
        public int Likes { get; set; }
        public DateTime DatetimeReview { get; set; } 

    }
}