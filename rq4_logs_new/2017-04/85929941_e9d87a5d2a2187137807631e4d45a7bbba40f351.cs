namespace WebshopProject.Migrations
{
    using Models;
    using System.Collections.Generic;
    using System.Data.Entity.Migrations;
    using System.Linq;

    internal sealed class Configuration : DbMigrationsConfiguration<WebshopProject.Models.WebshopContext>
    {
        public Configuration()
        {
            AutomaticMigrationsEnabled = true;
        }

        protected override void Seed(WebshopProject.Models.WebshopContext context)
        {
            //  This method will be called after migrating to the latest version.

            //  You can use the DbSet<T>.AddOrUpdate() helper extension method 
            //  to avoid creating duplicate seed data. E.g.
            //
            //    context.People.AddOrUpdate(
            //      p => p.FullName,
            //      new Person { FullName = "Andrew Peters" },
            //      new Person { FullName = "Brice Lambson" },
            //      new Person { FullName = "Rowan Miller" }
            //    );
            //
       
        var categorys = new List<Category>
            {
                new Category { CategoryName="Comedy", CategoryDescription= "Comedies are light-hearted plots consistently and deliberately designed to amuse and provoke laughter (with one-liners, jokes, etc.) by exaggerating the situation, the language, action, relationships and characters. ." },
                new Category { CategoryName="Action", CategoryDescription="Action films usually include high energy, big-budget physical stunts and chases, possibly with rescues, battles, fights, escapes, destructive crises (floods, explosions, natural disasters, fires, etc.), non-stop motion, spectacular rhythm and pacing, and adventurous, often two-dimensional 'good-guy' heroes (or recently, heroines) battling 'bad guys' - all designed for pure audience escapism. " }

            };
        categorys.ForEach(s => context.Category.AddOrUpdate(p => p.CategoryName, s));
            context.SaveChanges();

            var moviess = new List<Movie>
            {
                new Movie { MovieName = "Central Intelligence ", MovieDescription = " Central Intelligence is a 2016 American action comedy film directed by Rawson Marshall Thurber and written by Thurber, Ike Barinholtz and David Stassen." ,Cost=150, CategoryId = categorys.SingleOrDefault(i => i.CategoryName=="Comedy").Id,MovieImage= "Images/Movie Pic/Central Intelligence.jpg",Quantity=10 },
                new Movie { MovieName = "Jason Bourne ", MovieDescription = "Jason Bourne is a 2016 American action thriller film directed by Paul Greengrass written by Greengrass and Christopher Rouse. In this fifth installment of the Jason Bourne film series and direct sequel to 2007's The Bourne Ultimatum, Matt Damon reprises his role as the main character, former CIA assassin and psychogenic amnesiac Jason Bourne. " ,Cost=185, CategoryId = categorys.SingleOrDefault(i => i.CategoryName=="Action").Id ,MovieImage="Images/Movie Pic/Jason_Bourne_(film).jpg" ,Quantity=10},

                  };
        moviess.ForEach(s => context.Movie.AddOrUpdate(p => p.MovieName, s));
            context.SaveChanges();



        }
}
}