using System;

namespace Service.Activity.Domain.Models
{
    public class Activity
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public string Category { get; set; }
        public string Description { get; set; }
        public Guid UserId { get; set; }
        public DateTime CreatedAt { get; set; }
        
        protected Activity()
        {
            
        }

        public Activity(Guid id, Category categoty, Guid userId, string name, string description, DateTime createdAt)
        {
            Id = id;
            Category = categoty.Name;
            UserId = userId;
            Name = name.ToLowerInvariant();
            Description = description;
            CreatedAt = createdAt;
        }
    }
}