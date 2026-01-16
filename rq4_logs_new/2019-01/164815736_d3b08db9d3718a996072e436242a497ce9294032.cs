using System;
using System.Collections.Generic;
using System.Text;

namespace CRUD_OOP.Core.ValueObjects.Name
{
    public class AuthorName : ValueObject<AuthorName>, IName
    {
        public AuthorName(OneWordName firstName, OneWordName middleName, OneWordName lastName)
        {
            if (firstName == null) throw new ArgumentException("AuthorName firstName cannot be null.");
            if (lastName == null) throw new ArgumentException("AuthorName lastName cannot be null.");

            this.FirstName = firstName;
            this.MiddleName = middleName;
            this.LastName = lastName;
        }

        public OneWordName FirstName { get; private set; }
        public OneWordName MiddleName { get; private set; }
        public OneWordName LastName { get; private set; }

        public string GetFullName()
        {
            if (MiddleName == null)
            {
                return $"{FirstName.Value} {LastName.Value}";
            }
            return $"{FirstName.Value} {MiddleName.Value} {LastName.Value}";
        }
    }
}