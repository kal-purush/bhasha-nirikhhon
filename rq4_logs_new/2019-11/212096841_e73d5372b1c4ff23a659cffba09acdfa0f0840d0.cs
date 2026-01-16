using System;
using System.Collections.Generic;
using System.Text;

namespace Tetfuza
{
    public enum BlockColor
    {
        Background = 0,
        BlockColor1 = 1,
        BlockColor2 = 2,
        BlockColor3 = 3,
        Topout = 4
    }
    public class GridSpace
    {
        public BlockColor Color { get; set; } = BlockColor.Background;
        public bool IsLockedDownBlock { get; set; } = false;

        public GridSpace()
        {

        }
        public GridSpace(BlockColor color)
        {
            Color = color;
        }
        public GridSpace(GridSpace original)
        {
            Color = original.Color;
            IsLockedDownBlock = original.IsLockedDownBlock;
        }
    }
}