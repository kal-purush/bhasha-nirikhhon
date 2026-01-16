using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataLoggerAppV1
{
    internal class DbAiData
    {
        public double Ai0 { get; set; }
        public double Ai1 { get; set; }
        public double Ai2 { get; set;}
        public double Ai3 { get; set;}
        public double Ai4 { get; set;}
        public double Ai5 { get; set;}
        public double Ai6 { get; set;}
        public double Ai7 { get; set;}
        public short AiPercent0 { get; set; }
        public short AiPercent1 { get;set; }
        public short AiPercent2 { get;set; }
        public short AiPercent3 { get;set; }
        public short AiPercent4 { get;set; }
        public short AiPercent5 { get;set; }
        public short AiPercent6 { get;set; }
        public short AiPercent7 { get;set; }
        public int CountDownCycleTime { get;set; }
        public int CountDownBypassTime { get;set; }
    }
}