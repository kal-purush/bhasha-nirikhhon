using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TheMathMaze
{
    class MazeMul
    {
    }

    class MulEquation : BaseEquation
    {
        public MulEquation(string str) : base(str)
        {
            if (method != METHOD.MUL)
            {
                clear();
            }
            init();
        }
        public MulEquation(BaseEquation _be) : base(_be.equation_console)
        {
            if (method != METHOD.MUL)
            {
                clear();
            }
            init();
        }

        private void init()
        {
        }
    }
}