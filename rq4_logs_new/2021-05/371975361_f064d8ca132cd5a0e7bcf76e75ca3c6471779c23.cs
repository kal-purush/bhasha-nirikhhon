using System;
using System.ComponentModel.DataAnnotations;

namespace MarktGuruTask.Entities
{
    public abstract class BaseEntity
    {
        [Key]
        public int Id { get; set; }
        public DateTime DateCreated { get; set; }
    }
}