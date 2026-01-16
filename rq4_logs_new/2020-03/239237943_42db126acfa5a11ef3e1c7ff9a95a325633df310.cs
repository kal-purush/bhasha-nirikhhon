using MazeKata;
using Xunit;

namespace TestProject2
{
    public class MazeTests
    {
        [Fact]
        public void It_Should_Return_A_Maze_Of_Correct_Size_When_Given_String()
        {
            string a = ".W.\n" +
                       ".W.\n" +
                       "...";
            
            var mazeGenerator = new MazeGenerator();

            var maze = mazeGenerator.Generate(a);
            
            Assert.Equal(3, maze.Rows());
            
            
        }
    }
}