using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EtlPackage
{
    public class MarkDownWriter : StreamWriter
    {
        public MarkDownWriter(string path) : base(path)
        {
            NestedLevel = 1;
        }
        public int NestedLevel { get; set; }
        public void WriteTitle(string title)
        {
            string markup = new string('#', NestedLevel);
            this.Write(string.Format($"{markup} {title}\n"));
        }
        public void WriteCode(string code)
        {
            this.Write(string.Format($"`{code}`\n"));
        }
        public override Encoding Encoding
        {
            get { return Encoding.Default; }
        }
    }
}