using CRUD_OOP.Core.Objects;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace CRUD_OOP.Tests.Core.Objects
{
    public class Author_Create
    {
        [Theory]
        [InlineData(1, "V")]
        [InlineData(33, "Lev")]
        [InlineData(33, "Gustave Flaubert")]
        public void SetCorrectValue_Should_BeSet(int idIndDb, string name)
        {
            Author author = Author.Create(idInDB: idIndDb, name: name);
            Assert.Equal(idIndDb, author.IdInDB);
            Assert.Equal(name, author.Name);
        }

    }
}