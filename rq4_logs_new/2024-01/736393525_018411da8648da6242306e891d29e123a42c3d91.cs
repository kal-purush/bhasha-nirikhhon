using Data.Entities;
using Data.Interfaces;
using Data.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Data.Data
{
    public class UnitOfWork : IUnitOfWork
    {
        private readonly OnlineLibriaryDBContext dbContext;
        private Repository<Author> authorRepository;

        private Repository<Book> bookRepository;

        private Repository<Genre> genreRepository;

        private Repository<User> userRepository;

        public Repository<Author> AuthorRepository => authorRepository ??= new Repository<Author>(dbContext);

        public Repository<Book> BookRepository => bookRepository ??= new Repository<Book>(dbContext);

        public Repository<Genre> GenreRepository => genreRepository ??= new Repository<Genre>(dbContext);

        public Repository<User> UserRepository => userRepository ??= new Repository<User>(dbContext);

        public UnitOfWork(OnlineLibriaryDBContext dBContext)
        {
            this.dbContext = dBContext;
        }

        public Task SaveAsync()
        {
            throw new NotImplementedException();
        }
    }
}