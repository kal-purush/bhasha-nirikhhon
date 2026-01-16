namespace Homies.Data.Configurations
{
    using Models;

    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Metadata.Builders;

    public class CategorySeederEntityConfiguration : IEntityTypeConfiguration<Category>

    {
        public void Configure(EntityTypeBuilder<Category> builder)
        {
            builder.HasData(this.GenerateCategories());
        }

        private Category[] GenerateCategories()
        {
            ICollection<Category> categories = new HashSet<Category>();

            Category category = new Category();
           
            category = new Category()
            {
                Id = 1,
                Name = "Animals"
            };
            categories.Add(category);

            category = new Category()
            {
                Id = 2,
                Name = "Fun"
            };
            categories.Add(category);

            category = new Category()
            {
                Id = 3,
                Name = "Discussion"
            };
            categories.Add(category);

            category = new Category()
            {
                Id = 4,
                Name = "Work"
            };
            categories.Add(category);

            return categories.ToArray();
        }
    }
}