using PuzzleSolver.Abstractions;

namespace PuzzleSolver.Core.UnitTest.Mockups
{
    internal class MockupSudokuService : BaseMockup, IPuzzleService
    {
        private readonly GeneratorTemplate generator;
        private readonly MockupSudokuValidator validator;
        private readonly ResolverTemplate resolver;

        public MockupSudokuService()
        {
            validator = new MockupSudokuValidator();
            generator = new MockupSudokuGenerator(validator);
            resolver = new MockupSudokuResolver(validator);
        }

        public bool CheckState(string puzzleJson)
        {
            throw new System.NotImplementedException();
        }

        public PuzzleTemplate Generate(int knownFields) => generator.Generate(knownFields);

        public string Resolve(string puzzleJson)
        {
            throw new System.NotImplementedException();
        }
    }
}