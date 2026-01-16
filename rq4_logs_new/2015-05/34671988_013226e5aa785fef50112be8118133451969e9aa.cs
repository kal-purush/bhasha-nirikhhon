using System.Collections.Generic;

namespace LanguageTranslator.Definition
{
    class FieldDef
    {
        public AccessLevel AccessLevel = AccessLevel.Private;
        public TypeSpecifier Type = null;
        public string Name = string.Empty;
        public Expression Initializer = null;
        public string Brief = string.Empty;

        /// <summary>
        /// パーサー内部処理用
        /// </summary>
        internal Microsoft.CodeAnalysis.CSharp.Syntax.FieldDeclarationSyntax Internal = null;

        public override string ToString()
        {
            return string.Format("FieldDef {0}", Name);
        }
    }

    class PropertyDef
    {
        public AccessLevel AccessLevel = AccessLevel.Private;
        public TypeSpecifier Type = null;
        public string Name = string.Empty;
        public AccessorDef Getter = null;
        public AccessorDef Setter = null;
        public string Brief = string.Empty;

        /// <summary>
        /// パーサー内部処理用
        /// </summary>
        internal Microsoft.CodeAnalysis.CSharp.Syntax.PropertyDeclarationSyntax Internal = null;

        public override string ToString()
        {
            return string.Format("PropertyDef {0}", Name);
        }
    }

    class AccessorDef
    {
        public AccessLevel AccessLevel = AccessLevel.Private;
        public Statement Body = null;

        /// <summary>
        /// パーサー内部処理用
        /// </summary>
        internal Microsoft.CodeAnalysis.CSharp.Syntax.AccessorDeclarationSyntax Internal = null;
    }

    class MethodDef
    {
        public AccessLevel AccessLevel = AccessLevel.Private;
        public TypeSpecifier ReturnType = null;
        public string Name = string.Empty;
        public string Brief = string.Empty;
        public List<ParameterDef> Parameters = new List<ParameterDef>();
        public List<Statement> Body = new List<Statement>();

        /// <summary>
        /// パーサー内部処理用
        /// </summary>
        internal Microsoft.CodeAnalysis.CSharp.Syntax.MethodDeclarationSyntax Internal = null;

        public override string ToString()
        {
            return string.Format("MethodDef {0}", Name);
        }
    }

    class ParameterDef
    {
        public TypeSpecifier Type = null;
        public string Name = string.Empty;
        public string Brief = string.Empty;

        public override string ToString()
        {
            return string.Format("ParameterDef {0}", Name);
        }
    }

    class OperatorDef
    {
        public AccessLevel AccessLevel = AccessLevel.Private;
        public TypeSpecifier ReturnType = null;
        public string Operator = string.Empty;
        public List<ParameterDef> Parameters = new List<ParameterDef>();
        public List<Statement> Body = new List<Statement>();

        public override string ToString()
        {
            return string.Format("ParameterDef {0}", Operator);
        }
    }

    class ConstructorDef
    {
        public AccessLevel AccessLevel = AccessLevel.Private;
        public string Brief = string.Empty;
        public List<ParameterDef> Parameters = new List<ParameterDef>();
        public List<Statement> Body = new List<Statement>();

        public override string ToString()
        {
            return string.Format("ConstructorDef");
        }
    }

    class DestructorDef
    {
        public List<Statement> Body = new List<Statement>();

        public override string ToString()
        {
            return string.Format("DestructorDef");
        }
    }
}