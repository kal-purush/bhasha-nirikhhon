using System;
using System.ComponentModel.DataAnnotations;

namespace HRM.ApplicationCore.Model.Request
{
	public class CandidateRequestModel
	{
        [Required]
        public string FirstName { get; set; }
        [Required]
        public string LastName { get; set; }
        [Required]
        public string Phone { get; set; }
        [Required]
        public string Email { get; set; }
        [Required]
        public string currentAddress { get; set; }
        public string? ResumeUrl { get; set; }
    }
}
