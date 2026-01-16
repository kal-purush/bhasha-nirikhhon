using System.Collections.Generic;

namespace Extrator.Model
{
    public class Conexoes
    {
        public string origem { get; set; }
        public string host { get; set; }
        public string database { get; set; }
        public string port { get; set; }
        public string user { get; set; }
        public string pwd { get; set; }
    }

    public class Config
    {
        public IList<Conexoes> conexoes { get; set; }
        public IList<string> url { get; set; }
    }
}