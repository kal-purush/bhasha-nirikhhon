using CRUD_OOP.Core.ValueObjects;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace CRUD_OOP.Tests.ValueObjects
{
    public class BookName_Ctor
    {
        [Fact]
        public void NameIsNull_Should_ThrowException()
        {
            void action() => new BookName(null);

            Assert.Throws<ArgumentException>((Action)action);
        }

        [Fact]
        public void NameIsEmpty_Should_ThrowException()
        {
            void action() => new BookName(string.Empty);

            Assert.Throws<ArgumentException>((Action)action);
        }

        [Theory]
        [InlineData("d")]
        [InlineData("War and Peace")]
        [InlineData("CLR via C#")]
        public void NameIsOk_Should_BeOk(string name)
        {
            BookName bookName = new BookName(name);

            Assert.Equal(name, bookName.Name);
        }
    }
}