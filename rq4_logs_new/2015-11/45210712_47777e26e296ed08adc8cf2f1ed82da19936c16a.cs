using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace doubleStuffed
{
    class Board
    {
        public int[][] Spaces;
        public Board() { }

        private void FlipToken() { }

        public bool CheckBoard() {return false; }
        public bool CheckSquare() { return false; }
        public void CommitMove() { }
        public bool FlipCheck(int x, int y, int dirX, int dirY) { return false; }
        public void FlipToken(int x, int y) { }
        public void InitBoard() { }
    }
}