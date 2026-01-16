using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xcaciv.Command.FileLoader;
using Xcaciv.Command.Interface;

namespace Xcaciv.Command.Tests.TestImpementations
{
    internal class CommandControllerTestHarness : CommandController
    {
        /// <summary>
        /// Command Manger
        /// </summary>
        public CommandControllerTestHarness() : base(new Crawler()) { }
        /// <summary>
        /// Command Manger
        /// </summary>
        public CommandControllerTestHarness(ICrawler crawler) : base(crawler) { }
  
        /// <summary>
        /// Command Manager constructor to specify restricted directory
        /// </summary>
        /// <param name="restrictedDirectory"></param>
        public CommandControllerTestHarness(ICrawler crawler, string restrictedDirectory) :  base(crawler, restrictedDirectory) { } 
        /// <summary>
        /// Command Manager test constructor
        /// </summary>
        /// <param name="packageBinearyDirectories"></param>
        public CommandControllerTestHarness(IVerfiedSourceDirectories packageBinearyDirectories) : base(packageBinearyDirectories) { }

        internal Dictionary<string, ICommandDescription> GetCommands()
        {
            return this.Commands;
        }
    }
}