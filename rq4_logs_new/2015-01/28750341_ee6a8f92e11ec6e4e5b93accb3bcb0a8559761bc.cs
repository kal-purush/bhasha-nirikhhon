using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SuperScript.Declarables;
using SuperScript.Modifiers;
using SuperScript.Templates.Declarables;
using SuperScript.Templates.Modifiers.Converters;

namespace SuperScript.Templates.jQueryTmpl.Modifiers.Converters
{
	/// <summary>
	/// Converts the 'Declarations' property of the specified <see cref="PreModifierArgs"/> into an instance of <see cref="PostModifierArgs"/> 
	/// where the 'Emitted' property contains the stringified JavaScript.
	/// </summary>
	public class JQueryTmplStringify : TemplateStringify
	{
		/// <summary>
		/// <para>Enumerates each <see cref="TemplateDeclaration"/> and converts the declarations into a string.</para>
		/// <para>If <see cref="TemplateStringify.PreCompile"/> is <c>true</c> then pre-compilation should occur in this method.</para>
		/// </summary>
		/// <param name="declarations">
		/// A generic collection of <see cref="DeclarationBase"/> implementations. Only implementations which are of type 
		/// <see cref="TemplateDeclaration"/> will be considered.
		/// </param>
		protected override string CollateTemplates(IEnumerable<DeclarationBase> declarations)
		{
			var templates = new StringBuilder();

			if (PreCompile)
			{
				var engine = new Jurassic.ScriptEngine();
				engine.Execute(Properties.Resources.jqueryTmplCompiler);

				foreach (TemplateDeclaration declaration in declarations.Where(d => d.GetType() == typeof(TemplateDeclaration)))
				{
					var precompiledTemplate = engine.CallGlobalFunction<string>("buildTmplFn", declaration.Template);

					templates.AppendFormat("$.template(\"{0}\", {1});\n", declaration.Name, precompiledTemplate);
				}
			}
			else
			{
				foreach (TemplateDeclaration declaration in declarations.Where(d => d.GetType() == typeof(TemplateDeclaration)))
				{
					templates.AppendLine(declaration.ToString());
				}
			}

			return templates.ToString();
		}


		/// <summary>
		/// Default constructor for <see cref="JQueryTmplStringify"/> which has default locations in which to check for the
		/// existence of the script file.
		/// </summary>
		public JQueryTmplStringify()
		{
			ScriptPath = "~/scripts/jquery.tmpl.min.js";

			if (String.IsNullOrWhiteSpace(ScriptContents))
			{
				ScriptPath = "~/scripts/jquery.tmpl.js";
			}
		}
	}
}