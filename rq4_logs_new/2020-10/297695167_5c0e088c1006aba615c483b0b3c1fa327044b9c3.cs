using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using Bogus;
using Bogus.Extensions;
using QuestStore.Core.Entities;
using QuestStore.Core.Enums;

namespace QuestStore.Infrastructure.Data
{
    public class FakeData
    {
        private const int FakesNumber = 10;
        public List<Quest> FakeQuests { get; }
        public List<Artifact> FakeArtifacts { get; }
        public List<Student> FakeStudents { get; }
        public List<StudentArtifact> FakeStudentArtifacts { get; }

        public FakeData()
        {
            var questId = 1;
            var questFaker = new Faker<Quest>()
                .StrictMode(false)
                .RuleFor(q => q.Id, f => questId++)
                .RuleFor(q => q.Name, f => f.Commerce.ProductName())
                .RuleFor(q => q.Description, f => f.Commerce.ProductDescription())
                .RuleFor(q => q.Reward, f => f.Random.Number(1000))
                .RuleFor(q => q.Type, f => f.PickRandom<QuestType>());
            FakeQuests = questFaker.Generate(FakesNumber);

            var artifactId = 1;
            var artifactFaker = new Faker<Artifact>()
                .StrictMode(false)
                .RuleFor(a => a.Id, f => artifactId++)
                .RuleFor(a => a.Name, f => f.Commerce.ProductName())
                .RuleFor(a => a.Description, f => f.Commerce.ProductDescription())
                .RuleFor(a => a.Cost, f => f.Random.Number(1000))
                .RuleFor(a => a.Quantity, f => f.Random.Number(10).OrNull(f, 0.5f));
            FakeArtifacts = artifactFaker.Generate(FakesNumber);

            var studentId = 1;
            var studentFaker = new Faker<Student>()
                .StrictMode(false)
                .RuleFor(s => s.Id, f => studentId++)
                .RuleFor(s => s.Name, f => f.Person.FirstName)
                .RuleFor(s => s.Description, f => f.Hacker.Phrase())
                .RuleFor(s => s.Surname, f => f.Person.LastName)
                .RuleFor(s => s.Email, f => f.Person.Email);
            FakeStudents = studentFaker.Generate(FakesNumber);

            var studentArtifactId = 1;
            var studentArtifactPairs = GenerateRandomPairs(
                FakeStudents.Select(fs => fs.Id).ToList(),
                FakeArtifacts.Select(fa => fa.Id).ToList(), FakesNumber);
            var studentArtifactFaker = new Faker<StudentArtifact>()
                .StrictMode(false)
                .RuleFor(sa => sa.Id, f => studentArtifactId++)
                .RuleFor(sa => sa.StudentId, f => studentArtifactPairs[studentArtifactId - 2].Item1)
                .RuleFor(sa => sa.ArtifactId, f => studentArtifactPairs[studentArtifactId - 2].Item2);
            FakeStudentArtifacts = studentArtifactFaker.Generate(FakesNumber);
        }

        public static List<(T1, T2)> GenerateRandomPairs<T1, T2>(List<T1> first, List<T2> second, int length)
        {
            if (length > first.Count * second.Count)
            {
                throw new ArgumentException("The length of return list must be smaller or equal to the product of the lengths of the lists used for the drawing");
            }

            var result = new HashSet<(T1, T2)>();
            var randomizer = new Randomizer();
            var i = 0;

            while (i < length)
            {
                var pair = (randomizer.ListItem(first), randomizer.ListItem(second));
                if (result.Add(pair))
                {
                    ++i;
                }
            }

            return result.ToList();
        }
    }
}