using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace TypeRacers.Client
{
    public class PlayerMessage : IMessage
    {
        int wpmProgress;
        int completedTextPercentage;
        string name;
        public PlayerMessage(int wpmProgress, int completedTextPercentage, string name)
        {
            this.wpmProgress = wpmProgress;
            this.completedTextPercentage = completedTextPercentage;
            this.name = name;
        }
        public byte[] ToByteArray()
        {
            return Encoding.ASCII.GetBytes(wpmProgress + "&" + completedTextPercentage + "$" + name + "#");
        }
    }
}