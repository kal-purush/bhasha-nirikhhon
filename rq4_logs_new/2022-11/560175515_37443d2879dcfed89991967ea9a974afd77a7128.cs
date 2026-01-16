using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IELTSpeaking.Stages
{
    interface IStage
    {
        Task Go();
        IStage NextStage();
    }
}