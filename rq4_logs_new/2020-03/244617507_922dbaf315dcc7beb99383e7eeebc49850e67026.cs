using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Lunabank.Data.Entities
{
    public partial class Accounts
    {
        [Key]
        public Guid AccountId { get; set; }
        [Required]
        [StringLength(50)]
        public string AccountNumber { get; set; }
        [Column(TypeName = "datetime")]
        public DateTime CreatedOn { get; set; }
        [Required]
        [StringLength(450)]
        public string UserId { get; set; }
        [Required]
        [StringLength(50)]
        public string AccountType { get; set; }
        [Required]
        [StringLength(50)]
        public string Status { get; set; }
        [Column(TypeName = "decimal(18, 4)")]
        public decimal Balance { get; set; }

        [ForeignKey(nameof(UserId))]
        [InverseProperty(nameof(AspNetUsers.Accounts))]
        public virtual AspNetUsers User { get; set; }
    }
}