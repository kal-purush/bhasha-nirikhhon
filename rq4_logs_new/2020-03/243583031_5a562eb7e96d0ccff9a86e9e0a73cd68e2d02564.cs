using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DefaultEcs.System;
using GigglyLib.Components;

namespace GigglyLib.Systems
{
    public class InputRecorder : ISystem<float>
    {
        public bool IsEnabled { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public void Dispose() { throw new NotImplementedException(); }

        public void Update(float state)
        {
            if (Game1.Player.Has<CMoveAction>())
            {
                var move = Game1.Player.Get<CMoveAction>();
                byte data =
                    move.DistX == -1 && move.DistY == 0 ? (byte)0 :
                    move.DistX == 1 && move.DistY == 0 ? (byte)1 :
                    move.DistX == 0 && move.DistY == -1 ? (byte)2 :
                    move.DistX == 0 && move.DistY == 1 ? (byte)3 :
                    byte.MaxValue;
                if (data == byte.MaxValue)
                    throw new Exception("Invalid move data");
                data = (byte)(data << (Game1.ReplayIntraByteCounter * 2));
                Game1.ReplayData[Game1.ReplayData.Count-1] = (byte)((int)Game1.ReplayData.Last() ^ (int)data);

                Game1.ReplayIntraByteCounter++;
                if (Game1.ReplayIntraByteCounter >= 4)
                {
                    Game1.ReplayIntraByteCounter = 0;
                    Game1.ReplayData.Add(0);
                }
            }
        }
    }
}