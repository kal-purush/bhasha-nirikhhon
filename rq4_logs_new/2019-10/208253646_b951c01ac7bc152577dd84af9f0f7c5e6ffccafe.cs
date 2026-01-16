using System;
using TheFipster.Munchkin.StashRepository;
using TheFipster.Munchkin.StashRepository.Decorators;
using TheFipster.Munchkin.StashRepository.Models;

namespace TheFipster.Munchkin.StashConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            var monster = new Monster("Zerschmetterling", 7);

            var monsterWriter = new Writer<Monster>();
            var auditedMonsterWriter = new WriteAuditing<Monster>(monsterWriter);
            var blah = new WriteEventEmitter<Monster>(auditedMonsterWriter);
            blah.Save(monster);

            var monsterReader = new Reader<Monster>();
            var cachedMonsterReader = new CachedReader<Monster>(monsterReader);

            cachedMonsterReader.GetAll();
            cachedMonsterReader.GetAll();

            Console.ReadKey();
        }




























































        private static void curseReadTests()
        {
            var curseReader = new Reader<Curse>();
            var cachedCurseReader = new CachedReader<Curse>(curseReader);

            cachedCurseReader.GetAll();
            cachedCurseReader.GetAll();
        }

        private static void monsterReadTests()
        {
            var monsterReader = new Reader<Monster>();
            var cachedMonsterReader = new CachedReader<Monster>(monsterReader);
            var monsterName = "Laufende Nase";
            cachedMonsterReader.GetAll();
            cachedMonsterReader.GetOne(monsterName);
            cachedMonsterReader.GetOne(monsterName);
        }

        private static void monsterWriteTests()
        {
            var monsterWriter = new Writer<Monster>();
            var auditedMonsterWriter = new WriteAuditing<Monster>(monsterWriter);
            var eventEmittingMonsterWriter = new WriteEventEmitter<Monster>(auditedMonsterWriter);

            var monster = new Monster("Zerschmetterling", 7);
            eventEmittingMonsterWriter.Save(monster);
        }
    }
}