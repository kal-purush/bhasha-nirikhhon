using System;
using System.Collections.Generic;
using System.Linq;
using MathLang.Extensions;

namespace MathLang
{
    public class CompilerSettings
    {
        public IEnumerable<string> FilesPaths { get; set; }
        public CodeGenerationTarget CodeGenerationTarget { get; set; }
        public bool PrintSyntaxTree { get; set; }
        public bool PrintAstTree { get; set; }
        public bool ShowJasminBytecode { get; set; }

        public static CompilerSettings ParseCompilerSettings(IEnumerable<string> files, IEnumerable<string> parameters)
        {
            CompilerSettings c = new CompilerSettings {FilesPaths = files.ToList()};
            parameters.ForEach(p =>
            {
                if (p.Contains("--target")) ProcessTargetSetting(c, p);
            });
            return c;
        }

        private static void ProcessTargetSetting(CompilerSettings settings, string targetSetting)
        {
            if (settings.CodeGenerationTarget != CodeGenerationTarget.None)
                throw new Exception("--target setting was already specified");
            switch (targetSetting)
            {
                case "--target=net":
                    settings.CodeGenerationTarget = CodeGenerationTarget.Dotnet;
                    break;
                case "--target=java":
                    settings.CodeGenerationTarget = CodeGenerationTarget.Java;
                    break;
                default:
                    throw new Exception($"target setting does not support value {targetSetting}");
            }
        }
    }

    public enum CodeGenerationTarget
    {
        None = 0,
        Java,
        Dotnet
    }
}