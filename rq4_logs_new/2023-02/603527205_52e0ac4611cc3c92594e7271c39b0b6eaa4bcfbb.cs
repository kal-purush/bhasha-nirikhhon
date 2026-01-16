using System;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace HRM.ApplicationCore.Entity
{
	public class Candidate
	{
		public int Id { get; set; }
        [Required]
        [Column(TypeName = "varchar(20)")]
        public string FirstName { get; set; }
        [Required]
        [Column(TypeName = "varchar(20)")]
        public string LastName { get; set; }
        [Required]
        [Column(TypeName = "varchar(10)")]
        [DataType(DataType.PhoneNumber)]
        public string Phone { get; set; }
        [Required]
        [Column(TypeName = "varchar(70)")]
        [DataType(DataType.EmailAddress)]
        public string Email { get; set; }
        [Required]
        [Column(TypeName = "varchar(100)")]
        public string currentAddress { get; set; }
        [Column(TypeName = "varchar(200)")]
        public string? ResumeUrl { get; set; }
	}
}
