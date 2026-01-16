using Microsoft.Build.Locator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.MSBuild;
using System;
using System.IO;
using System.Threading.Tasks;
using TechTalk.SpecFlow;

namespace Augurk.CSharpAnalyzer.Specifications.Support
{
    [Binding]
    public static class Hooks
    {
        public static string Solution;
        public static Workspace Workspace;

        [BeforeTestRun]
        public static async Task BeforeTestRun()
        {
            MSBuildLocator.RegisterDefaults();
            Solution = Path.Combine(AppContext.BaseDirectory, @"..\..\..\Analyzable Projects\Cucumis.sln");
            var msBuildWorkspace = MSBuildWorkspace.Create();
            await msBuildWorkspace.OpenSolutionAsync(Solution);
            Workspace = msBuildWorkspace;
        }
    }
}