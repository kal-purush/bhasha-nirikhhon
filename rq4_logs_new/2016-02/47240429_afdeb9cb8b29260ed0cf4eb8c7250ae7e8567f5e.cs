// <copyright file="Assembly_tests.cs" company="Nate Barbettini">
// Copyright (c) 2015 Nate Barbettini. Licensed under MIT.
// </copyright>

#if NET451
using System.IO;
using System.Reflection;
#endif

using System.Collections.Generic;
using FluentAssertions;
using Xunit;

namespace libpolyglot.Tests
{
    public class Assembly_tests
    {
        public static IEnumerable<object[]> TestAssemblies()
        {
            // Sadly, not enough heuristic data to properly guess on these (yet).
            yield return new object[] { "EmptyCS.dll", "EmptyCS", null };
            yield return new object[] { "EmptyCSMono.dll", "EmptyCS", null };

            // VB.NET - not supported on DNX yet :(
#if NET451
            yield return new object[] { "EmptyVB.dll", "EmptyVB", Language.Vb };
            yield return new object[] { "AnonymousAsyncVB.dll", "AnonymousAsyncVB", Language.Vb };
#endif

            // C#
            yield return new object[] { "AnonymousAsyncCS.dll", "AnonymousAsyncCS", Language.CSharp };
            yield return new object[] { "AnonymousAsyncCSMono.dll", "AnonymousAsyncCS", Language.CSharp };
            yield return new object[] { "DynamicCS.dll", "DynamicCS", Language.CSharp };
            yield return new object[] { "DynamicCSMono.dll", "DynamicCS", Language.CSharp };

            // F#
            yield return new object[] { "EmptyFSharp.dll", "EmptyFSharp", Language.FSharp };
        }

        [Theory]
        [MemberData(nameof(TestAssemblies))]
        public void When_analyzing(string fileName, string assemblyName, Language? expected)
        {
#if NET451
            var assembly = Assembly.LoadFrom($"..\\..\\..\\SampleTestAssemblies{Path.DirectorySeparatorChar}{fileName}");
#else
            var assembly = CustomAssemblyLoader.Load(fileName, assemblyName);
#endif

            var analyzer = new AssemblyAnalyzer(assembly, failSilently: false);

            analyzer.DetectedLanguage.Should().Be(expected);
        }
    }
}