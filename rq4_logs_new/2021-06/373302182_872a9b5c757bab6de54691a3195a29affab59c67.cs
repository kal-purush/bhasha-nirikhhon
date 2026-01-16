using System;
namespace NeutrackAPI.DTOs
{
    public class AllPatientReadDTO
    {
        public int Id { get; set; }
        public int UserId { get; set; }
        public double Height { get; set; }
        public double Weight { get; set; }
        public double Goal { get; set; }
        public int ActivityLevel { get; set; }
        public bool IsActive { get; set; }
        public string Email { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string FullName { get; set; }
        public string Gender { get; set; }
        public DateTime DateOfBirth { get; set; }
        public string PhoneNumber { get; set; }
        public double InitialWeight { get; set; }
    }
}