using Bookworm_Society_API.Models;

namespace Bookworm_Society_API.Data
{
    public class AuthorData
    {
        public static List<Author> Authors = new()
        {
            new() { Id = 1, FirstName = "Harper", LastName = "Lee" },
            new() { Id = 2, FirstName = "J.R.R.", LastName = "Tolkien" },
            new() { Id = 3, FirstName = "Frank", LastName = "Herbert" },
            new() { Id = 4, FirstName = "Gillian", LastName = "Flynn" },
            new() { Id = 5, FirstName = "Jane", LastName = "Austen" },
            new() { Id = 6, FirstName = "Markus", LastName = "Zusak" },
            new() { Id = 7, FirstName = "Stieg", LastName = "Larsson" },
            new() { Id = 8, FirstName = "Shirley", LastName = "Jackson" },
            new() { Id = 9, FirstName = "Paulo", LastName = "Coelho" },
            new() { Id = 10, FirstName = "Bram", LastName = "Stoker" },
            new() { Id = 11, FirstName = "J.D.", LastName = "Salinger" },
            new() { Id = 12, FirstName = "Cormac", LastName = "McCarthy" },
            new() { Id = 13, FirstName = "J.K.", LastName = "Rowling" },
            new() { Id = 14, FirstName = "George", LastName = "Orwell" },
            new() { Id = 15, FirstName = "F. Scott", LastName = "Fitzgerald" }
        };
    }
}