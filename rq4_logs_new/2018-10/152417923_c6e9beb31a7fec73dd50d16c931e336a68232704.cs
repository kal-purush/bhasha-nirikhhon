using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace _08_Badge_Challenge
{
    public class Badge
    {
        public Badge()
        {
            DoorName = new List<string>();
        }



        public int BadgeId { get; set; }
        public List<string> DoorName { get; set; }
    }      
}