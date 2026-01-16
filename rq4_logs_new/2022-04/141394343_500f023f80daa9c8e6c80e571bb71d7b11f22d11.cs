using McnLib.Blocks;
using McnLib.Sections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace McnLib.FileProcessing
{
    public class TextFileParser
    {
        TextFileReader fileReader = new TextFileReader();
        ILineParser[] parsers = { new SectionParser(), new BlockParser() };
        public void Parse(string filePath)
        {
            var lines = fileReader.ReadFile(filePath);
            foreach(var line in lines)
            {
                foreach(var parser in parsers)
                {
                    if(parser.ProcessLine(line))
                        continue;
                }
            }
        }

        public List<Block> GetBlocks()
        {
            return ((BlockParser)parsers[1]).Blocks;
        }
    }
}