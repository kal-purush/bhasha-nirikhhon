using System;
using Microsoft.EntityFrameworkCore;
using Xunit;


namespace DAB2_2RDB.Integration.Test
{


    public class RepositoryTests
    {
        private readonly Repository<Person> _uut;

        public RepositoryTests()
        {
            DbContext context = new Dab2_2RdbContext();
            _uut = new Repository<Person>(context);
        }

        [Fact]
        public void Repository_CanCreate_NoThrow()
        {
            Person be = new Person(){Id = 1};

            _uut.Create(be);

            Assert.Equal(_uut.Read(be.Id), be);
        }
    }
}