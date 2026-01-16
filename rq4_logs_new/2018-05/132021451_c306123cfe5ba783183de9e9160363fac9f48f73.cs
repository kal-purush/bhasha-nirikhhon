using Library.DLL.DataModels;
using System.Data.Entity;

namespace Library.DLL.Context
{
    public class LibraryDbContext : DbContext
    {
        public LibraryDbContext()
           : base("LibraryDB")
        {
        }
        public virtual DbSet<Book> Books { get; set; }
        public virtual DbSet<Author> Authors { get; set; }
    }
}