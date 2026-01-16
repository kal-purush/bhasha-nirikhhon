using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LifeTest
{
    [System.Serializable]
    public abstract class ContentBase
    {
        public int PosX;
        public int PosY;
        public char Icon { get; set; }

        public ContentBase()
        {

        }

        public ContentBase(int x, int y, char ico)
        {
            PosX = x;
            PosY = y;
            Icon = ico;
        }

        public static bool operator true(ContentBase content)
        {
            if (content != null)
                return true;
            else
                return false;
        }

        public static bool operator false(ContentBase content)
        {
            if (content == null)
                return false;
            else return true;
        }
    }
}