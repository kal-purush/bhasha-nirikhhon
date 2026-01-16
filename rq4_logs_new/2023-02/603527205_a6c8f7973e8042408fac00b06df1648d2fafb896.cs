using System;
using HRM.ApplicationCore.Entity;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace HRM.ApplicationCore.Model.Request
{
	public class JobRequirementRequestModel
	{
        public int Id { get; set; }
        [Required(ErrorMessage = "Title is required")]
        public string Title { get; set; }
        [Required(ErrorMessage = "Job Location is required")]
        public string JobLocation { get; set; }
        public int NoOfPosition { get; set; }
        public DateTime PostingDate { get; set; }
        public DateTime ClosingDate { get; set; }
        public bool IsActive { get; set; }
        [Required(ErrorMessage = "Description is required")]
        public string Description { get; set; }
        public int JobCategoryId { get; set; }


    }
}
