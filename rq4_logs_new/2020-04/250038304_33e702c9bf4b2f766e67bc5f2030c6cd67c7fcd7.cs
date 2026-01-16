using Newtonsoft.Json;
using System.IO;
using System.Collections.Generic;

namespace ODWai2.ODWaiCore.Models
{
    public class DetectionResult
    {
        public string class_name;
        public string bias;
        public int center_x;
        public int center_y;
        public int root_x;
        public int root_y;
        public int width;
        public int height;
        public string text;

        public DetectionResult()
        {
            class_name = bias = text = "";
            center_x = center_y = root_x = root_y = width = height = 0;
        }

        public static List<DetectionResult> from_json(string from_path)
        {
            string raw_json = File.ReadAllText(from_path);
            return JsonConvert.DeserializeObject<List<DetectionResult>>(raw_json);
        }
    }
}