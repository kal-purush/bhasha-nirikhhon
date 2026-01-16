namespace GolfApp
{
    using System;
    using System.Data.Entity;
    using System.Linq;

    public class GolfDB : DbContext
    {
        // Your context has been configured to use a 'GolfDB' connection string from your application's 
        // configuration file (App.config or Web.config). By default, this connection string targets the 
        // 'GolfApp.GolfDB' database on your LocalDb instance. 
        // 
        // If you wish to target a different database and/or database provider, modify the 'GolfDB' 
        // connection string in the application configuration file.
        public GolfDB()
            : base("name=GolfDB")
        {
        }

        // Add a DbSet for each entity type that you want to include in your model. For more information 
        // on configuring and using a Code First model, see http://go.microsoft.com/fwlink/?LinkId=390109.

        public virtual DbSet<Game> Games { get; set; }
        public virtual DbSet<Hole> Holes { get; set; }
        public virtual DbSet<User> Users { get; set; }
    }

    //public class MyEntity
    //{
    //    public int Id { get; set; }
    //    public string Name { get; set; }
    //}
}