using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;
using System.Xml.Serialization;

namespace LifeTest
{

    [XmlInclude(typeof(Grass))]
    [XmlInclude(typeof(Grass_2))]
    [XmlInclude(typeof(Herbivorous_1))]
    public class ContentBase
    {
        public int PosX;
        public int PosY;
        public char Icon { get; set; }

        public ContentBase()
        {

        }
        [XmlElement]
        public virtual IBehavior Behavior { get; }

        public ContentBase(int x, int y, char ico)
        {
            PosX = x;
            PosY = y;
            Icon = ico;
        }

        public static bool operator true(ContentBase content)
        {
            return content != null;
        }

        public static bool operator false(ContentBase content)
        {
            return content != null;
        }
    }
}