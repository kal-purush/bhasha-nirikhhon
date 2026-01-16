using System;
using System.Collections.Generic;
using System.Linq;
using Jusfr.Persistent.Mongo;
using MongoDB.Bson;
using PersistentDemo.Mongo;
using Xunit;

namespace Jusfr.Persistent.Tests {
    public class MongoRepositoryTest {
        private const Byte Max_lvl = (Byte)100;
        private readonly PubsContext _context;

        public MongoRepositoryTest() {
            _context = new PubsContext();
            _context.Database.DropCollection("Job");
            var repo = new MongoRepository<Job>(_context);
            repo.Create(new Job {
                Id = ObjectId.Parse("574c0b01ca3c5fa8b8cfa0c8"),
                Max_lvl = Max_lvl,
            });
        }

        [Fact]
        public void Retrive_via_primaryKey() {
            var repo = new MongoRepository<Job>(_context);
            var theFirstOne = repo.Retrive(ObjectId.Parse("574c0b01ca3c5fa8b8cfa0c8"));
            Assert.NotNull(theFirstOne);
        }

        [Fact]
        public void Retrive_via_primaryKey_list() {
            var repo = new MongoRepository<Job>(_context);
            var allKeys = repo.All.Select(x => x.Id).ToArray()
                .Select(x => (Object)x).ToArray(); 
            var allItems = repo.Retrive(keys: allKeys).ToArray();

            Assert.NotEmpty(allKeys);
            Assert.Equal(allKeys.Length, allItems.Length);
        }

        [Fact]
        public void Retrive_via_field() {
            var repo = new MongoRepository<Job>(_context);
            var theSpecials = repo.Retrive("Max_lvl", (Byte)Max_lvl);
            Assert.NotEmpty(theSpecials);
        }

        [Fact]
        public void Retrive_via_field_list() {
            var repo = new MongoRepository<Job>(_context);
            var allKeys = repo.All.Select(x => x.Id).ToArray();
            var allItems = repo.Retrive(x => x.Id, allKeys).ToArray();

            Assert.NotEmpty(allKeys);
            Assert.Equal(allKeys.Length, allItems.Length);
        }

        [Fact]
        public void Retrive_via_expression() {
            var repo = new MongoRepository<Job>(_context);
            var theSpecials = repo.Retrive(j => j.Max_lvl, (Byte)Max_lvl);
            Assert.NotEmpty(theSpecials);
        }

        [Fact]
        public void Retrive_via_expression_contains() {
            var repo = new MongoRepository<Job>(_context);
            var allKeys = repo.All.Select(x => x.Id).ToArray();
            var allItems = repo.All.Where(r => allKeys.Contains(r.Id)).ToArray();

            Assert.NotEmpty(allKeys);
            Assert.Equal(allKeys.Length, allItems.Length);
        }

        [Fact]
        public void Retrive_via_queryable() {
            var repo = new MongoRepository<Job>(_context);
            var query = repo.All.Where(r => r.Max_lvl == Max_lvl);
            var theSpecials = repo.Fetch(_ => query);
            Assert.NotEmpty(theSpecials);
        }
    }
}