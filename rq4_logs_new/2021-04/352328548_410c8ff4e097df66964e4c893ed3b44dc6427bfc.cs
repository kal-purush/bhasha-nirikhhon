using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace Data_.Entities
{
    public class RefreshToken
    {
        [Key]
        public string Token { get; set; }
        public DateTime CreatedTime { get; set; }
        public DateTime ExpiryDate { get; set; }
        [ForeignKey(nameof(UserId))]
        public User User { get; set; }
        public Guid UserId { get; set; }
    }
}