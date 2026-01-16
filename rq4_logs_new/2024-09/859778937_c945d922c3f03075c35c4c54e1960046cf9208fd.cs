using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hackathon.AI.Models
{
    public class VideoTestModel
    {
        public string Query { get; set; }
        public double Confidence { get; set; }
        public double MinimumConfidence { get; set; }
    }
}