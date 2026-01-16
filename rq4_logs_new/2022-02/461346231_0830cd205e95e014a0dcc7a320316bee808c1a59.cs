using System;
using System.Collections.Generic;

namespace placeMarket.api.Models
{
    public class Category
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string Url { get; set; }
        public string ImageUrl { get; set; }
        public string MiniatureUrl { get; set; }
        public string VideoUrl { get; set; }
        public string Color { get; set; }
        public int? Author_id { get; set; }
        public virtual ICollection<Category> ChildrenCategories { get; set; }
        public virtual Category FatherCategory { get; set; }
        public int? FatherCategoryId { get; set; }
        public DateTime DeleteDate { get; set; }
        public int DeleteUserId { get; set; }
        public int CreationUserId { get; set; }
        public DateTime CreationDate { get; set; }
        public int UpdateUserId { get; set; }
        public DateTime UpdateDate { get; set; }

    }
}