using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace PulsePI.Models
{
    public class BiometridData
    {
        public int Id { get; set; }
        public Account account { get; set; }
        [ForeignKey("Account")]
        public int accountId { get; set; }
        [StringLength(25)]
        public double height { get; set; }
        public double weight { get; set; }
        public char sex { get; set; }
        public DateTime Date { get; set; }
    }
}