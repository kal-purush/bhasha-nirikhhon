using BusinessLogic.Utils;
using Data.Data;
using Data.Entities;

namespace API.Utils
{
    public static class Seeder
    {
        public static void SeedData(OnlineLibriaryDBContext context)
        {
            var genres = CreateGenres();
            var authors = CreateAuthors();
            var books = CreateBooks();
            var users = CreateUsers();
            var securePasswordHasher = new SecurePasswordHasher();

            for (int i = 0; i < 10; i++)
            {
                users[i].Books.Add(books[i]);
                users[i].Password = securePasswordHasher.Hash(users[i].Password);
            }

            context.Genres.AddRange(genres);
            context.Authors.AddRange(authors);
            context.Books.AddRange(books);
            context.Users.AddRange(users);
            context.SaveChanges();
        }

        public static List<Genre> CreateGenres()
        {
            var result = new List<Genre>();
            for (int i = 1; i <= 10; i++)
            {
                result.Add(new Genre
                {
                    Id = i,
                    Name = $"Genre{i}"
                });
            }
            return result;
        }
        public static List<Author> CreateAuthors()
        {
            var result = new List<Author>();
            for (int i = 1; i <= 10; i++)
            {
                result.Add(new Author
                {
                    Id = i,
                    FirstName = $"FirstName{i}",
                    SurName = $"SurName{i}",
                    Country = $"Country{i}",
                    DateOfBirth = DateTime.Now
                });
            }
            return result;
        }
        public static List<Book> CreateBooks()
        {
            var result = new List<Book>();
            for (int i = 1; i <= 10; i++)
            {
                result.Add(new Book
                {
                    Id = i,
                    Title = $"Title{i}",
                    Description = $"Description{i}",
                    AuthorId = i,
                    GenreId = i,
                    Year = DateTime.Now.Year,
                    BookContent = Array.Empty<byte>()
                });
            }
            return result;
        }
        public static List<User> CreateUsers()
        {
            var result = new List<User>();
            for (int i = 1; i <= 10; i++)
            {
                result.Add(new User
                {
                    Id = i,
                    Username = $"Username{i}",
                    FirstName = $"FirstName{i}",
                    SurName = $"SurName{i}",
                    Email = $"email{i}@gmail.com",
                    Password = $"Password{i}",
                    Books = new List<Book>(),
                    ProfilePicture = Array.Empty<byte>()
                });
            }
            return result;
        }
    }
}