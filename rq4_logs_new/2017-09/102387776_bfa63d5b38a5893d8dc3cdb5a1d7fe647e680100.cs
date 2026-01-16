using System;
using CoolBytes.Core.Extensions;

namespace CoolBytes.Core.Models
{
    public class Author
    {
        public int Id { get; private set; }
        public string FirstName { get; private set; }
        public string LastName { get; private set; }
        public Photo Photo { get; private set; }
        public string About { get; private set; }

        public Author(string firstName, string lastName, string about)
        {
            firstName.IsNotNullOrWhiteSpace();
            lastName.IsNotNullOrWhiteSpace();
            about.IsNotNullOrWhiteSpace();

            FirstName = firstName;
            LastName = lastName;
            About = about;
        }

        public Author(string firstName, string lastName, string about, Photo photo)
        {
            firstName.IsNotNullOrWhiteSpace();
            lastName.IsNotNullOrWhiteSpace();
            about.IsNotNullOrWhiteSpace();
            photo.IsNotNull();

            FirstName = firstName;
            LastName = lastName;
            Photo = photo;
            About = about;
        }

        private Author() { }
    }
}