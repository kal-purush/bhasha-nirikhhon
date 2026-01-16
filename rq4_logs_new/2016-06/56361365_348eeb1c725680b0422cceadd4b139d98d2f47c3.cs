using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Harcourts.Face.WebsiteRepository;
using Harcourts.Face.WebsiteService.Lookups;
using NUnit.Framework;

namespace Harcourts.Face.UnitTest.Repositories
{
    [TestFixture]
    internal class PersonRepositoryTests
    {
        [Test, Explicit]
        public void RebuildCollection()
        {
            var lookup = new JsonPersonLookup();
            var persons = lookup.AsEnumerable().ToArray();
            var repo = new PersonRepository();



            repo.ExecuteCollection(x=>x.InsertMany(persons))
        }
    }
}