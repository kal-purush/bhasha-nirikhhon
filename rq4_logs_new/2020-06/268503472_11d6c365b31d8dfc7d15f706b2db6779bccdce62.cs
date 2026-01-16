using System;
using System.Collections.Generic;
using System.Linq;

namespace DocumentFactory
{
    class Program
    {
        static void Main(string[] args)
        {
            var random = new Random();

            var factories = new List<DocumentFactory>
            {
                new ApplicationFactory(),
                new CurriculumVitaeFactory(),
                new ReportFactory(),
                new StoryFactory()
            };
            factories.OrderBy(f => random.Next()).ToList().ForEach(f =>
            {
                // Factory returns shared interface, not specific implementation, since it's derived from shared abstract factory
                IDocument document = f.CreateDocument();
                document.Print();
            });
        }
    }
}