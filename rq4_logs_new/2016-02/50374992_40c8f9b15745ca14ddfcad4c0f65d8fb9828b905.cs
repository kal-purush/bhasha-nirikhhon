using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lego.EV3.Shared
{
    public class DriveCommandCount
    {
        public DriveCommand Command { get; set; }
        public int Count { get; set; }
    }

    public class LegoState
    {
        public List<DriveCommandCount> AllVotes { get; set; }
        public int SensorDistance { get; set; }
        public LegoState()
        {
            AllVotes = new List<DriveCommandCount>();
            foreach (DriveCommand cmd in Enum.GetValues(typeof(DriveCommand)))
            {
                AllVotes.Add(new DriveCommandCount() { Command = cmd, Count = 0 });
            }
        }
    }
}