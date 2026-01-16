using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace LanguageTranslator.Parser
{
	class Utils
	{
		public static string ToStr(INamespaceSymbol ns)
		{
			if (ns.Name == "") return ns.Name;
			return ns.ToString();
		}
	}
}