using System;
using System.Collections.Generic;
using System.Text;

namespace CRUD_OOP.Core.Objects
{
    public class Author : Entity<Guid>
    {
        public int? IdInDB { get; private set; }
        public string Name { get; private set; }

        private Author(Guid id, int? idInDB, string name): base(id: id)
        {
            this.IdInDB = idInDB;
            this.Name = name;
        }

        public static Author Create(int? idInDB, string name)
        {
            return new Author(
                id: Guid.NewGuid(),
                idInDB: idInDB,
                name: name);
        }
        
    }
}