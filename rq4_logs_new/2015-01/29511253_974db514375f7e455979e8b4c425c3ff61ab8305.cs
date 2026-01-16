using MiNET.Worlds;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MiNET.Mobiles
{
    public class Mobile
    {
        public virtual int EntityId { get; set; }
        public virtual int Type { get; set; }
        public virtual Position3D Position { get; set; }
        public virtual float BodyYaw { get; set; }
        public virtual Level _level { get; set; }
        public virtual DateTime LastUpdatedTime { get; set; }


        public Mobile(Level level)
        {
            _level = level;
            LastUpdatedTime = DateTime.Now;
        }

        public virtual void SpawnTo(Position3D position)
        {
            Position = position;
            _level.AddMobile(this);
        }

        //Other mobile stuff...
    }
}