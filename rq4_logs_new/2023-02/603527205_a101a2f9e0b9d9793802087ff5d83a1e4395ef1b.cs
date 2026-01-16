using System;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace HRM.ApplicationCore.Model.Response
{
	public class CandidateResponseModel
	{
        public int Id { get; set; }
        
        public string FirstName { get; set; }
        
        public string LastName { get; set; }
        
        public string Phone { get; set; }
        
        public string Email { get; set; }
        
        public string currentAddress { get; set; }

        public string? ResumeUrl { get; set; }
    }
}

