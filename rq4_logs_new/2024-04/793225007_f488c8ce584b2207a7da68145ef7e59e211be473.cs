namespace CodeAnalyzer;

public class Analyzer
{
    private static readonly HashSet<string> Types = ["int", "long", "bool", "string", "double", "var", "char"];
    private static readonly HashSet<string> Remove = ["private", "static", "public", "protected"];
    private static readonly string[] Cycles = ["for", "while", "foreach"];
    private static readonly string[] NotVar = [":", "?"];
    private static readonly string[] Operators = ["+", "-", "/", "*", "!", "{", "}", "<", "<=", ">", ">=", "*=", "/=", "break", "continue" ];

    public List<string> MethodsDefs { get; } = []; // объявления методов
    public List<string> MethodsCalls { get; } = []; // все вызовы методов
    public List<string> VariablesDefs { get; } = []; // объявления переменных
    public Dictionary<string, int> MethodsCallsCount { get; } = []; // счётчик вызовов методов     
    public Dictionary<string, int> MethodsCallsAsOperandsCount { get; } = []; // счётчик вызовов методов в качестве операндов
    public Dictionary<string, int> VariablesUsageCount { get; } = []; // счётчик использования переменных     
    public Dictionary<string, int> LiteralsUsageCount { get; } = []; // счётчик использования литералов     
    public Dictionary<string, int> OperatorsUsageCount { get; } = []; // счётчик использования операторов     
    public List<string> IfStatements { get; } = []; // условные констуркции     
    public List<string> CycleStatements { get; } = []; // циклы     

    public override string ToString()
    {
        var firstColumnWidth = 4;
        var secondColumnWidth = 32;
        int operatorsCount = 1;
        int operandsCount = 1;
        int numberOfOperators = OperatorsUsageCount.Count;
        int numberOfOperands = VariablesUsageCount.Count + LiteralsUsageCount.Count + MethodsCallsAsOperandsCount.Count;
        int operatorsSum = OperatorsUsageCount.Values.Sum();
        int operandsSum = VariablesUsageCount.Values.Sum() + LiteralsUsageCount.Values.Sum() + MethodsCallsAsOperandsCount.Values.Sum();
        return $"Операторы:\n" +
               string.Join("\n", OperatorsUsageCount.Select(op=>$"{operatorsCount++.AddSpacesRightToLen(firstColumnWidth)}\t{op.Key.AddSpacesRightToLen(secondColumnWidth)}\t{op.Value}")) +
               $"\nКол-во: {numberOfOperators}\tсумма:{operatorsSum}\n\nОперанды:\n" +
               string.Join("\n", VariablesUsageCount.Select(variable=>$"{operandsCount++.AddSpacesRightToLen(firstColumnWidth)}\t{variable.Key.AddSpacesRightToLen(secondColumnWidth)}\t{variable.Value}")) +
               "\n" +
               string.Join("\n", LiteralsUsageCount.Select(literal=>$"{operandsCount++.AddSpacesRightToLen(firstColumnWidth)}\t{literal.Key.AddSpacesRightToLen(secondColumnWidth)}\t{literal.Value}")) +
               "\n" +
               string.Join("\n", MethodsCallsAsOperandsCount.Select(method=>$"{operandsCount++.AddSpacesRightToLen(firstColumnWidth)}\t{(method.Key+ "(..)").AddSpacesRightToLen(secondColumnWidth)}\t{method.Value}")) +
               $"\nКол-во: {numberOfOperands}\tсумма:{operandsSum}" +
               "\n\n\n\nОбъявления методов:\n" + 
               string.Join("\n", MethodsDefs) +
               "\n\nОбъявления переменных:\n" +
               string.Join("\n", VariablesDefs) +
               "\n\nВызовы методов:\n" +
               string.Join("\n", MethodsCalls) +
               "\n\nУсловия if:\n" +
               string.Join("\n", IfStatements) +
               "\n\nЦиклы:\n" +
               string.Join("\n", CycleStatements);
    }

    public double CalcVolume()
    {
        int numberOfOperators = OperatorsUsageCount.Count;
        int numberOfOperands = VariablesUsageCount.Count + LiteralsUsageCount.Count + MethodsCallsAsOperandsCount.Count;
        int operatorsSum = OperatorsUsageCount.Values.Sum();
        int operandsSum = VariablesUsageCount.Values.Sum() + LiteralsUsageCount.Values.Sum() + MethodsCallsAsOperandsCount.Values.Sum();
        int fullUnicNumber = numberOfOperators + numberOfOperands; // Уникальных операторов и операндов
        int fullSum = operatorsSum + operandsSum; // всего операторов и операндов
        double volume = fullSum * Math.Log2(fullUnicNumber);
        return volume;
    }

    public Analyzer(string code)
    {
        foreach (var s in Remove)
            code = code.Replace(s, "");
        var lines = code.Split(Environment.NewLine).Select(s => s.Trim());

        foreach (var codeLine in lines)
        {
            ParseMethodsAndVariablesAndLiterals(codeLine);
        }
    }

    private void ParseMethodsAndVariablesAndLiterals(string codeLine, bool methodIsOperand = false)
    {
        codeLine = codeLine.EndsWith(';') ? codeLine[..codeLine.LastIndexOf(';')].Trim() : codeLine; // убрать точку с запятой
        var line = SyncTrimBrackets(codeLine.Trim()).Trim();
        var commentIndex = line.IndexOf("//", StringComparison.Ordinal);
        if (commentIndex >= 0)
            line = line[..commentIndex].Trim();
        if (line.Length == 0)
            return;
        var isStartWithType = Types.Any(t => line.StartsWith(t + " ") || line.StartsWith(t + "? ") ||  line.StartsWith("out " + t));
        if (isStartWithType) // начинается с типа данных
        {
            if (line.Contains('=')) // создание переменной
            {
                var varDef = line[..line.IndexOf('=')].Trim();
                VariablesDefs.Add(line);
                ParseMethodsAndVariablesAndLiterals(line[(line.IndexOf('=') + 1)..], true);
            }
            else if (line.Contains('(')) // содержит круглую скобку - значит объявление метода
            {
                MethodsDefs.Add(line);
            }
        }
        else if (Operators.Any(op => line.Equals(op)))
        {
            if (!OperatorsUsageCount.TryAdd(line, 1))
                OperatorsUsageCount[line]++;
        }
        else if (line.StartsWith("switch"))
        {
            if (!OperatorsUsageCount.TryAdd("switch", 1))
                OperatorsUsageCount["switch"]++;
            var parameter = line[(line.IndexOf('(') + 1)..line.LastIndexOf(')')];
            ParseMethodsAndVariablesAndLiterals(parameter, true);
        }
        else if (line.StartsWith("case"))
        {
            if (!OperatorsUsageCount.TryAdd("case", 1))
                OperatorsUsageCount["case"]++;
            var parameter = line[(line.IndexOf(' ') + 1)..line.LastIndexOf(':')];
            ParseMethodsAndVariablesAndLiterals(parameter, true);
        }
        else if (line.StartsWith("return"))
        {
            if (!OperatorsUsageCount.TryAdd("return", 1))
                OperatorsUsageCount["return"]++;
            if (line.Contains(' '))
            {
                var parameter = line[(line.IndexOf(' ') + 1)..];
                ParseMethodsAndVariablesAndLiterals(parameter, true);
            }
        }
        else if(line.StartsWith("else"))
        {
            if (!OperatorsUsageCount.TryAdd("else", 1))
                OperatorsUsageCount["else"]++;
            ParseMethodsAndVariablesAndLiterals(line[4..], true);
        }
        else if (line.StartsWith("if"))
        {
            IfStatements.Add(line);
            if (!OperatorsUsageCount.TryAdd("if", 1))
                OperatorsUsageCount["if"]++;
            var condition = line[(line.IndexOf('(') + 1)..line.LastIndexOf(')')];
            ParseMethodsAndVariablesAndLiterals(condition, true);
        }
        else if (Cycles.Any(s => line.StartsWith(s)) || line.StartsWith('}') && line.Contains("while"))
        {
            var cycle = Cycles.First(c => line.Contains(c));
            if (!OperatorsUsageCount.TryAdd(cycle, 1))
                OperatorsUsageCount[cycle]++;
            CycleStatements.Add(line);
        }
        else if ((line.StartsWith('"') || line.StartsWith("$\"") || line.StartsWith("@\"")) && line.EndsWith('"') || line.StartsWith('\'') && line.EndsWith('\'') ||
                 double.TryParse(line, out _) || bool.TryParse(line, out _) || line == "null")
        {
            if (!LiteralsUsageCount.TryAdd(line, 1))
                LiteralsUsageCount[line]++;
        }
        else if (line.Contains('(') && !line[..line.IndexOf('(')].Contains(' ') &&
                 (line.EndsWith(')') || line.EndsWith(");"))) // вызов метода
        {
            MethodsCalls.Add(line);
            var name = line[..line.IndexOf('(')];
            if (!MethodsCallsCount.TryAdd(name, 1))
                MethodsCallsCount[name]++;
            if (!OperatorsUsageCount.TryAdd("()", 1))
                OperatorsUsageCount["()"]++;
            var parameters = line[(line.IndexOf('(') + 1)..line.LastIndexOf(')')]
                .Split(',', StringSplitOptions.RemoveEmptyEntries);
            if (methodIsOperand)
            {
                if (!MethodsCallsAsOperandsCount.TryAdd(name, 1))
                    MethodsCallsAsOperandsCount[name]++;
            }
            
            foreach (var parameter in parameters)
                ParseMethodsAndVariablesAndLiterals(parameter, true);
        }
        else
        {
            var words = line.Split(' ', StringSplitOptions.RemoveEmptyEntries).ToList();
            try
            {
                for (var i = 0; i < words.Count; i++)
                {
                    var word = words[i];
                    if (word.Count(ch => ch == '\"') % 2 != 0)
                    {
                        i++;
                        while (word.Count(ch => ch == '\"') % 2 != 0)
                        {
                            word += " " + words[i];
                            words.RemoveAt(i);
                        }

                        i -= 2;
                        words[i + 1] = word;
                    }
                    else if (word.Count(ch => ch == '(') != word.Count(ch => ch == ')'))
                    {
                        i++;
                        while (word.Count(ch => ch == '(') != word.Count(ch => ch == ')'))
                        {
                            word += " " + words[i];
                            words.RemoveAt(i);
                        }

                        i -= 2;
                        words[i + 1] = word;
                    }
                    else if (word.EndsWith('('))
                    {
                        i++;
                        while (!word.EndsWith(')'))
                        {
                            word += " " + words[i];
                            words.RemoveAt(i);
                        }

                        i -= 2;
                        words[i + 1] = word;
                    }
                }
            }
            catch (Exception)
            {
                return;
            }

            foreach (var word in words)
            {
                if (word != line)
                    ParseMethodsAndVariablesAndLiterals(word, true);
                else if (Operators.All(op=>word.Contains(op) == false) && NotVar.All(nv=>word!=nv) && !VariablesUsageCount.TryAdd(word, 1))
                    VariablesUsageCount[word]++;
            }
        }
    }

    private static string SyncTrimBrackets(string str)
    {
        if (str.Length < 2 || str[0] != '(' || str[^1] != ')')
            return str;

        var countBrackets = 0; //текущий уровень скобки
        for (var i = 1; i < str.Length - 1; i++)
        {
            if (str[i] == '(')
                countBrackets++;
            else if (str[i] == ')')
                countBrackets--;

            if (countBrackets == -1)
                return str;
        }

        if (countBrackets != 0)
            return str;
        var answer = str.Remove(str.Length - 1, 1).Remove(0, 1);
        var nextAnswer = SyncTrimBrackets(answer);
        return nextAnswer;
    }
}