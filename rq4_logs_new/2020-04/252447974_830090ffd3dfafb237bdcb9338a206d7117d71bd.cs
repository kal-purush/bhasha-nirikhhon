using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;

namespace App.Models
{
    [Table("Borrows")]
    public class Borrow
    {
        [Key]
        public int BorrowId { get; set; }

        [Required]
        public User Librarian { get; set; }

        public User ReturnLibrarian { get; set; }

        [Required]
        public User User { get; set; }

        [Required]
        public Book Book { get; set; }

        [Required]
        [DataType(DataType.DateTime)]
        public DateTime BorrowTime { get; set; }

        [DataType(DataType.DateTime)]
        public DateTime? ReturnTime { get; set; }

        [Required]
        public BorrowStatus Status { get; set; }

    }

    public enum BorrowStatus { Active, Inactive }
}