using PuzzleSolver.Abstractions;

namespace PuzzleSolver.Core.UnitTest.Mockups
{
    internal class MockupSudokuController : BaseMockup, IPuzzleController
    {
        private readonly IGenerator generator;
        private readonly IValidator validator;
        private readonly IResolver resolver;

        public MockupSudokuController()
        {
            generator = new MockupSudokuGenerator();
            validator = new MockupSudokuValidator();
            resolver = new MockupSudokuResolver();
        }
        public PuzzleTemplate Generate(int knownFields) => generator.Generate(knownFields);
    }
}