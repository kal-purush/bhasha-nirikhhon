using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Runtime.Remoting.Messaging;
using System.Security.AccessControl;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace LineCommander.Test
{
    [TestClass]
    public class LineCommanderTest
    {
        internal class MyOptions
        {
            public const string SportDefault = "Basketball";

            public Command RunMe { get; }
            public Command RunMeToo { get; }
            public Option Walk { get; }
            public Option File { get; }
            public Option Names { get; }
            public Option Sport { get; }

            public MyOptions()
            {
                RunMe = new Action("runme");
                RunMeToo = new Action("RunMeToo");
                DoWalk = new Option("walk", OptionValueType.None);
                FilePath = new Option("file", OptionValueType.Single);
                Names = new Option("names", OptionValueType.List);

                // help text
                RunMe.HelpText("Run me");
                RunMeToo.HelpText("Run me aswell");
                DoWalk.HelpText("Walk, don't run");
                FilePath.HelpText("");
                Names.HelpText("");

                // examples
                FilePath.ExampleText("C:\\Path\file.txt");
                Names.ExampleText("John");

                // ... or inline
                Sport = new Option("sport", OptionValueType.Single, SportDefault)
                    .HelpText("")
                    .ExampleText("Cricket");

            }
        }

        [TestMethod]
        public void ParseSuccess()
        {
            var opts = new MyOptions();

            var argSet = new ArgumentSet()
                .Required(opts.RunMe)
                .Required(opts.File)
                .Optional(opts.Flag)
                .Optional(opts.Names)
                .Optional(opts.Sport)
                .OnMatch(() =>
                {
                    opts.RunMe.IsPresent.ShouldBeTrue();

                    opts.File.Value.ShouldBe(@"C:\some\path\file.txt");
                    // value is convenience to calling Values.Single()
                    opts.File.Value.ShouldBe(@"C:\some\path\file.txt");
                    opts.File.Values.Single().ShouldBe(@"C:\some\path\file.txt");
                    
                    // optionl flag, must test if present
                    if (!opts.Sport.IsPresent)
                    {
                        // default value has been assigned
                        opts.Sport.Value.ShouldBe(MyOptions.SportDefault);
                    }

                    // safe to enumerate an optional list
                    opts.Names.Values.Any("Jill").ShouldBeTrue();
                    opts.Names.Values.Any("Bob").ShouldBeTrue();
                }),

            var args = new string[] {"RunMe", "--Names", "Bob", "Jill", "--file", @"C:\some\path\file.txt"};
            LineCommander.Parse(args, argSet)
                .OnError(bestMatch =>
                {
                    Log.Error($"No matching argument set: did you mean '{bestMatch}'");
                });
    }
}
}