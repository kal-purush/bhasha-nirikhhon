using Data.Data;
using Data.Entities;
using Data.Repositories;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OnlineLibriaryTests.Utils
{
    public class TestUtilities
    {
        public static Repository<T> CreateRepository<T>() where T : BaseEntity
        {
            var context = new OnlineLibriaryDBContext(new DbContextOptionsBuilder<OnlineLibriaryDBContext>()
                .EnableSensitiveDataLogging()
                .UseInMemoryDatabase(databaseName: "Test_Database").Options, ensureDeleted: true);
            SeedData(context);
            return new Repository<T>(context);
        }

        public static void SeedData(OnlineLibriaryDBContext context)
        {
            var genres = CreateGenres();
            var authors = CreateAuthors();
            var books = CreateBooks();
            var users = CreateUsers();

            for(int i = 0; i < 10; i++)
            {
                users[i].Books.Add(books[i]);
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
                    LastName = $"LastName{i}",
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
                    BookContent = new byte[0]
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
                    FirstName = $"FirstName{i}",
                    LastName = $"LastName{i}",
                    Email = $"Email{i}",
                    Password = $"Password{i}",
                    DateOfRegistration = DateTime.Now,
                    Books = new List<Book>(),
                    ProfilePicture = new byte[0]
                });
            }
            return result;
        }
    }
}