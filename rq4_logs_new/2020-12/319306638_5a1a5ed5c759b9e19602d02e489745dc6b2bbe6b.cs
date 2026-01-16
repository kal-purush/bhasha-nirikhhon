using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommandPattern
{
    interface Command
    {
        void execute();
    }

    class WebRequestCommand : Command
    {
        private String line;

        public WebRequestCommand(String line)
        {
            this.line = line;
        }

        public void execute()
        {
            var api = new API();
            var data1 = api.getData(line);
            Console.WriteLine($"Got data {data1}");
        }
    }
}