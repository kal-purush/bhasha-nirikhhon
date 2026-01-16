using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace JoyOI.Monitor.Models
{
    public class Graph
    {
        public string Title { get; set; }
        public List<GraphData> Data { get; set; }
    }

    public class GraphData {
        public string Title { get; set; }
        public List<DataPoint> data { get; set; }
    }

    public class DataPoint
    {
        public long Timestamp { get; set; }
        public double Value { get; set; }
    }
}