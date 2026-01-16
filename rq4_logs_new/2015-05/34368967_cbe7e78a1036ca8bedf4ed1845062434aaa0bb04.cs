using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sharpAdvising
{
    public class LocalGraphNode
    {
        public LocalGraphNode()
        {
            children = new List<LocalGraphNode>();
        }
        public List<int> groupID;
        public String group;
        public string departmentID;
        public string numberID;
        public List<LocalGraphNode> children;
        public int allCourseIndex;
    }
}