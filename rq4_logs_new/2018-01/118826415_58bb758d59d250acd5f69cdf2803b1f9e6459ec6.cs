using System;
using System.Collections.Generic;
using System.Text;
using Cake.Core.Diagnostics;
using Cake.Core.IO;
using Cake.FtpDeploy.Services;
using Cake.Testing.Fixtures;
using NSubstitute;

namespace Cake.FtpDeploy.Tests
{
    public class FtpDeployFixture<TSettings> : ToolFixture<TSettings> where TSettings : FtpDeploySettings, new()
    {

        private readonly ICakeLog _log;
        private readonly IWebService _testWebService;

        public FtpDeployFixture()
            : base("ftpdeploy")
        {
            _log = Substitute.For<ICakeLog>();
            _testWebService = new TestWebService();
        }

        protected override void RunTool()
        {
            var ftpService = new FtpService(_log, _testWebService);
            var tool = new FtpDeployRunner<TSettings>(FileSystem, Environment, ProcessRunner, Tools, ftpService);
            tool.Execute(Settings);
        }
    }
}