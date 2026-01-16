using McnLib.FileReader;

namespace McnLib.Blocks
{
    public class BlockParser
    {
        TextFileReader fileReader = new TextFileReader();

        List<Block> GetBlocks(string filePath)
        {
            var blocks = new List<Block>();
            var fileLines = fileReader.ReadFile(filePath);
            Block? nextBlock = null;
            foreach (var line in fileLines)
            {
                var trimmedText = line.Text.Trim();
                if (trimmedText.StartsWith(BlockConfig.Begin))
                {
                    nextBlock = new Block();
                    nextBlock.FileLines.Add(line);
                }
                else if (trimmedText.EndsWith(BlockConfig.End))
                {
                    if (nextBlock != null)
                    {
                        nextBlock.FileLines.Add(line);
                        blocks.Add(nextBlock);
                        nextBlock = null;
                    }
                }
            }
            return blocks;
        }
    }
}