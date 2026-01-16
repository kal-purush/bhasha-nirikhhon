using ScriptFunction.Instructions;
using ScriptFunction.Instructions.Operators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ScriptFunction
{
    public class BasicScriptParser
    {
        private static Dictionary<Regex, Func<Match, Instruction>> regexActions;
        private static Dictionary<Regex, Func<Match, Instruction>> userRegexActions;
        private static Dictionary<Regex, Macro> macros;
        private static Dictionary<Regex, Macro> userMacros;

        private Dictionary<string, ScriptDelegate> delegates;

        public BasicScriptParser()
        {
            delegates = new Dictionary<string, ScriptDelegate>();

            userMacros = new Dictionary<Regex, Macro>();
            userRegexActions = new Dictionary<Regex, Func<Match, Instruction>>();
        }

        static BasicScriptParser()
        {
            regexActions = new Dictionary<Regex, Func<Match, Instruction>>();
            macros = new Dictionary<Regex, Macro>();

            RegisterCommandRegex("^//", m => null, regexActions);

            #region Commands
            RegisterCommand("sto", @"(?<index>(\w|\d)+)", Store);
            RegisterCommand("ld.arg", @"(?<index>\d+)", LoadArgument);
            RegisterCommand("ld.var", @"(?<index>(\w|\d)+)", LoadVariable);
            RegisterCommand("retobj", m => new Return(false));
            RegisterCommand("ret", m => new Return(true));
            RegisterCommand("pop", m => new Pop());
            RegisterCommand("lbl", @"(?<label>\w+)", Label);
            RegisterCommand("jmp", @"(?<label>\w+)", Jump);
            RegisterCommand("branch", @"(?<label>\w+)", Branch);
            RegisterCommand("swap", m => new Swap());
            RegisterCommand("trap", m => new Trap());
            RegisterCommand("try", @"(?<label>\w+)", Try);
            RegisterCommand("trynot", m => new TryNot());
            RegisterCommand("dup", m => new Duplicate());
            RegisterCommand("nop", m => new Nop());
            RegisterCommand("reti", m => new ReturnCall());
            RegisterCommand("call", @"(?<label>\w+)", Call);
            RegisterCommand("async", @"(?<label>\w+)\,(?<args>\d+)", Async);
            #endregion
        }

        #region Instructions
        private static Instruction Async(Match arg)
        {
            var args = int.Parse(arg.Groups["args"].Value);
            return new Async(arg.Groups["label"].Value, args);
        }

        private static Instruction Call(Match arg)
        {
            var label = arg.Groups["label"].Value;
            return new Call(label);
        }

        private static Instruction Try(Match arg)
        {
            var name = arg.Groups["label"].Value;
            return new Try(name);
        }

        private static Instruction Branch(Match arg)
        {
            var name = arg.Groups["label"].Value;
            return new Branch(name);
        }

        private static Instruction Jump(Match arg)
        {
            var name = arg.Groups["label"].Value;
            return new Jump(name);
        }

        private static Instruction Label(Match arg)
        {
            var name = arg.Groups["label"].Value;
            return new Label(name);
        }

        private static Instruction Store(Match arg)
        {
            var name = arg.Groups["index"].Value;
            return new Store(name);
        }

        private static Instruction LoadVariable(Match arg)
        {
            var name = arg.Groups["index"].Value;
            return new LoadVariable(name);
        }

        private static Instruction LoadArgument(Match arg)
        {
            var index = int.Parse(arg.Groups["index"].Value);
            return new LoadArgument(index);
        }
        #endregion

        #region Register
        protected static void RegisterCommand(string command, Func<Match, Instruction> action)
        {
            RegisterCommandRegex(string.Format("^{0}$", UpperLowerRegex(command)), action, regexActions);
        }

        protected static void RegisterCommand(string command, string paramRegex, Func<Match, Instruction> action)
        {
            RegisterCommandRegex(string.Format("^{0} {1}$", UpperLowerRegex(command), paramRegex), action, regexActions);
        }

        protected static void RegisterCommandRegex(string regex, Func<Match, Instruction> action, Dictionary<Regex, Func<Match, Instruction>> regexActions)
        {
            regexActions[new Regex(regex)] = action;
        }

        protected static void RegisterMacro(string command, string macro)
        {
            RegisterMacro(command, new MArg[] { }, macro);
        }

        protected static void RegisterMacroRegex(string regex, IEnumerable<MArg> args, string macro, Dictionary<Regex, Macro> macros)
        {
            macros[new Regex(regex)] = new Macro()
            {
                MacroText = macro,
                Arguments = args,
            };
        }

        protected static void RegisterMacro(string command, IEnumerable<MArg> args, string macro)
        {
            var regexStr = "";

            if (!args.Any())
            {
                regexStr = string.Format("^{0}$", UpperLowerRegex(command));
            }
            else
            {
                regexStr = string.Format(@"^{0}\ {1}$", UpperLowerRegex(command),
                    string.Join(@"\,", args.Select(o => o.Regex)));
            }
            RegisterMacroRegex(regexStr, args, macro, macros);
        }

        public void RegisterUserCommand(string command, Func<Match, Instruction> action)
        {
            RegisterCommandRegex(string.Format("^{0}$", UpperLowerRegex(command)), action, userRegexActions);
        }

        public void RegisterUserCommand(string command, string paramRegex, Func<Match, Instruction> action)
        {
            RegisterCommandRegex(string.Format("^{0} {1}$", UpperLowerRegex(command), paramRegex), action, userRegexActions);
        }

        public void RegisterUserMacro(string command, string macro)
        {
            RegisterUserMacro(command, new MArg[] { }, macro);
        }

        public void RegisterUserMacro(string command, IEnumerable<MArg> args, string macro)
        {
            var regexStr = "";

            if (!args.Any())
            {
                regexStr = string.Format("^{0}$", UpperLowerRegex(command));
            }
            else
            {
                regexStr = string.Format(@"^{0}\ {1}$", UpperLowerRegex(command),
                    string.Join(@"\,", args.Select(o => o.Regex)));
            }
            RegisterMacroRegex(regexStr, args, macro, userMacros);
        }

        public void RegisterUserFuncCommand(string command, Func<ScriptEnvironment, object> func)
        {
            RegisterUserCommand(command, m => new FuncInstruction(func));
        }

        public void RegisterUserFuncCommand(string command, string paramRegex,
            Func<ScriptEnvironment, object> func)
        {
            RegisterUserCommand(command, paramRegex, m => new FuncInstruction(func));
        }

        public void RegisterUserActionCommand(string command, Action<ScriptEnvironment> action)
        {
            RegisterUserCommand(command, m => new ActionInstruction(action));
        }

        public void RegisterUserActionCommand(string command, string paramRegex,
            Action<ScriptEnvironment> action)
        {
            RegisterUserCommand(command, paramRegex, m => new ActionInstruction(action));
        }

        protected static string UpperLowerRegex(string txt)
        {
            var ret = new StringBuilder();
            foreach (var c in txt)
            {
                if (char.IsUpper(c) || char.IsLower(c))
                {
                    ret.AppendFormat("[{0}{1}]", char.ToUpperInvariant(c), char.ToLowerInvariant(c));
                }
                else if (char.IsNumber(c))
                {
                    ret.Append(c);
                }
                else
                {
                    ret.AppendFormat("\\{0}", c);
                }
            }
            return ret.ToString();
        }
        #endregion

        private IEnumerable<string> PreProcess(string line, out Regex usedRegex)
        {
            var m = macros
                .Concat(userMacros)
                .Select(o => new Tuple<Match, Macro, Regex>(o.Key.Match(line), o.Value, o.Key))
                .FirstOrDefault(o => o.Item1.Success);
            if (m != null)
            {
                var tmp = m.Item2.MacroText;
                foreach (var arg in m.Item2.Arguments)
                {
                    var g = m.Item1.Groups[arg.Name];
                    tmp = tmp.Replace(string.Format("${0}$", arg.Name), g.Value);
                }
                usedRegex = m.Item3;
                return tmp.Split('\n')
                    .Select(o => o.Trim());
            }
            else
            {
                usedRegex = null;
                return null;
            }
        }

        private ScriptDelegate CreateDelegate(string[] lines)
        {
            var list = new List<Instruction>();
            for (var i = 0; i < lines.Length; i++)
            {
                var line = lines[i];
                if (!string.IsNullOrWhiteSpace(line))
                {
                    var usedRegexs = new List<Regex>();
                    var macroLines = new List<string>();
                    macroLines.Add(line);
                    do
                    {
                        var macroLine = macroLines[0].Trim();
                        macroLines.RemoveAt(0);
                        Regex usedRegex;
                        var newMacroLines = PreProcess(macroLine, out usedRegex);
                        if (newMacroLines != null)
                        {
                            macroLines.AddRange(newMacroLines);
                            if(usedRegexs.Contains(usedRegex))
                            {
                                throw new InvalidOperationException(
                                    string.Format("Cyclic macro in code. Regex: {0}", usedRegex.ToString()));
                            }
                            else
                            {
                                usedRegexs.Add(usedRegex);
                            }
                        }
                        else
                        {
                            var t = regexActions
                               .Concat(userRegexActions)
                               .Select(o2 => new Tuple<Match, Func<Match, Instruction>>(o2.Key.Match(macroLine), o2.Value))
                               .FirstOrDefault(o2 => o2.Item1.Success);
                            if(t == null)
                            {
                                throw new InvalidOperationException(
                                    string.Format("Invalid command [#{2}: {0} / {1}]", macroLine, line.Trim(), i));
                            }
                            var ins = t.Item2(t.Item1);
                            if (ins != null)
                            {
                                ins.CodeLine = line.TrimEnd();
                                ins.LineNumber = i;
                                list.Add(ins);
                            }
                        }
                    }
                    while (macroLines.Any());
                }
            }
            return new ScriptDelegate(list);
        }

        public ScriptDelegate Create(string txt)
        {
            if (delegates.ContainsKey(txt))
            {
                return delegates[txt];
            }
            var d = CreateDelegate(txt.Split('\n'));
            delegates[txt] = d;
            return d;
        }

        public object Invoke(string txt, params object[] args)
        {
            var d = Create(txt);
            return d.DynamicInvoke(args);
        }

        public T Invoke<T>(string txt, params object[] args)
        {
            return (T)Invoke(txt, args);
        }

        public void ClearCache()
        {
            delegates.Clear();
        }

        #region Macro
        protected struct Macro
        {
            public string MacroText { get; set; }
            public IEnumerable<MArg> Arguments { get; set; }
        }
        #endregion
    }
}