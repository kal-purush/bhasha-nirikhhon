using FluentAssertions;
using System;
using System.IO;
using TheFipster.Munchkin.GameDomain;
using Xunit;

namespace TheFipster.Munchkin.GameStorage.LiteDb.UnitTest
{
    public class MonsterStoreTest : IDisposable
    {
        private Repository<Monster> _repository;
        private MonsterStore _monsterStore;

        public MonsterStoreTest()
        {
            _repository = new Repository<Monster>();
            _monsterStore = new MonsterStore(_repository);
        }

        public void Dispose()
        {
            _repository.Dispose();
            File.Delete(Repository<Monster>.Filename);
        }

        [Fact]
        public void AddAndGetMonster_ResultsInAllMonsterBeingReturned_Test()
        {
            // Arrange
            var zerschmetterling = new Monster("Zerschmetterling", 5);
            var plastikSoldaten = new Monster("Plastiksoldaten", 1);

            // Act
            _monsterStore.Add(zerschmetterling);
            _monsterStore.Add(plastikSoldaten);
            var actualMonsters = _monsterStore.Get();

            // Assert
            actualMonsters.Should().HaveCount(2);
        }

        [Fact]
        public void AddAndGetMonsterByName_ResultsInMonsterBeingReturned_Test()
        {
            // Arrange
            var monster = new Monster("Zerschmetterling", 5);

            // Act
            _monsterStore.Add(monster);
            var actualMonster = _monsterStore.Get(monster.Name);

            // Assert
            actualMonster.Level.Should().Be(monster.Level);
        }
    }
}