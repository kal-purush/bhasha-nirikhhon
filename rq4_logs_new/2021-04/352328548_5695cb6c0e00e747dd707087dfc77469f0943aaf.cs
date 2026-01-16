using Data_.Entities;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace Data_
{
    public class Achievement: BaseEntity
    {
        [Required]
        [MaxLength(70)]
        public string Name { get; set; }
        [Required]
        [MaxLength(250)]
        public string Description { get; set; }
        public uint Xp { get; set; }
        [Required]
        public Guid IconId { get; set; }
        public ICollection<UserAchievement> UserAchievements { get; set; }

    }
}