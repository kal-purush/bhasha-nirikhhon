using AgendaApi.Entities;
using Microsoft.EntityFrameworkCore;

namespace AgendaApi.Data
{
    public class AgendaContext: DbContext
    {
        public DbSet<User> Users { get; set; }
        public DbSet<Contact> Contacts { get; set; }

        public AgendaContext(DbContextOptions<AgendaContext> options) : base(options) 
        {

        }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            User karen = new User()
            {
                Id = 1,
                FirstName = "Karen",
                LastName = "Lasot",
                Password = "Pa$$w0rd",
                Email = "karenbailapiola@gmail.com",
                UserName = "karenpiola"
            };
            User luis = new User()
            {
                Id = 2,
                FirstName = "Luis Gonzalez",
                LastName = "Gonzales",
                Password = "lamismadesiempre",
                Email = "elluismidetotoras@gmail.com",
                UserName = "luismitoto"
            };

            Contact jaimitoC = new Contact()
            {
                Id = 1,
                Name = "Jaimito",
                CellPhoneNumber = 341457896,
                Description = "Plomero",
                TelephoneNumber = null,
                UserId = karen.Id,
            };

            Contact pepeC = new Contact()
            {
                Id = 2,
                Name = "Pepe",
                CellPhoneNumber = 34156978,
                Description = "Papa",
                TelephoneNumber = 422568,
                UserId = luis.Id,
            };

            Contact mariaC = new Contact()
            {
                Id = 3,
                Name = "Maria",
                CellPhoneNumber = 011425789,
                Description = "Jefa",
                TelephoneNumber = null,
                UserId = karen.Id,
            };

            modelBuilder.Entity<Contact>().HasData(mariaC, pepeC, jaimitoC);
            modelBuilder.Entity<User>().HasData(luis, karen);

            modelBuilder.Entity<User>().HasMany<Contact>(u => u.Contacts).WithOne(c => c.User);

            base.OnModelCreating(modelBuilder);

        }
    }
}