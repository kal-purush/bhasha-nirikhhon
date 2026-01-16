using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Microsoft.CSharp;

namespace Advent2015.Solutions
{
    class Day7
    {

        public string Transpile(string instruction)
        {
            var template = "public const ushort {0} = unchecked((ushort)({1}));";
            instruction = instruction.Replace("NOT ", "~").Replace("OR", "|").Replace("AND", "&")
                .Replace("LSHIFT", "<<").Replace("RSHIFT", ">>").Replace("->","=")
                .Replace("as","@as").Replace("if","@if").Replace("do", "@do").Replace("is", "@is").Replace("in", "@in");

            var parts = instruction.Split('=');
            return string.Format(template, parts[1].Trim(), parts[0].Trim());
        }

        public ushort BuildSolver(string[] instructions, string register)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"public class Logic{register} {{");
            
            foreach (var instruction in instructions)
            {
                sb.AppendLine(Transpile(instruction));
            }
             
            sb.AppendLine("public ushort GetValue(){");
            sb.AppendLine($"return {register};");
            sb.AppendLine(" }");
            sb.AppendLine("}");
            //Console.WriteLine(sb.ToString());
            var csc = new CSharpCodeProvider(new Dictionary<string, string>() { { "CompilerVersion", "v4.0" } });
            var parameters = new CompilerParameters(new[] { "mscorlib.dll", "System.Core.dll" }, $"Advent{register}.dll", true);

            var results = csc.CompileAssemblyFromSource(parameters, sb.ToString());
            var type = results.CompiledAssembly.GetType($"Logic{register}");
            dynamic logic = Activator.CreateInstance(type);
            
            return (ushort)logic.GetValue();
        }
    }
    
}