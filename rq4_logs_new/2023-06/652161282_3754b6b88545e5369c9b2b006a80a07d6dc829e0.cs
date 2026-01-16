using System;
using System.Collections.Generic;

namespace Recipe_Organizer_PRN211.Models
{
    public partial class Category
    {
        public Category()
        {
            Recipes = new HashSet<Recipe>();
        }

        public int CategoryId { get; set; }
        public string Title { get; set; } = null!;
        public string Description { get; set; } = null!;
        public int ParentCategoryId { get; set; }

        public virtual ICollection<Recipe> Recipes { get; set; }
    }
}