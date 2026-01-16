using Homework.Interfaces;
using Homework.Models;
using Homework.Utils;
using System.IO;

namespace Homework.Implementations
{
    public class ApplicationsFileReader: IApplicationsReader
    {
        readonly string _path;

        public ApplicationsFileReader(string path)
        {
            _path = path;
        }

        public Array<Application> Read()
        {
            var lines = new Array<string>(File.ReadAllLines(_path));

            lines.PopFirst();

            int identifier = 1;

            return lines.Map((string line) => new Application(identifier++, line));
        }
    }
}