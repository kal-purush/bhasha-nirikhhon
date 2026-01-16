using System.Collections.Generic;
using MathLang.Tree.Nodes.Declarations;
using MathLang.Tree.Nodes.Enums;

namespace MathLang.Tree.Semantics
{
    public class AttributeBuilder
    {
        private AttributeDeclaration Attribute { get; set; }

        public string Name { get; private set; }

        public List<FunctionVariableDeclarationParameter> Parameters { get; } =
            new List<FunctionVariableDeclarationParameter>();

        public AttributeBuilder(Nodes.Program program)
        {
            Attribute = new AttributeDeclaration(program, program.Scope);
        }

        public AttributeDeclaration Build()
        {
//            attr.ParameterNodes.Add(
//                new FunctionVariableDeclarationParameter(attr, attr.Scope)
//                {
//                    ReturnType = ReturnType.String,
//                    Name = "ReferencePath",
//                    Index = 0
//                });
//            attr.Name = "JavaRef";
            Attribute.Name = Name;
            Attribute.ParameterNodes.AddRange(Parameters);
            return Attribute;
        }

        public AttributeBuilder WithName(string name)
        {
            Name = name;
            return this;
        }

        public AttributeBuilder WithParameter(ReturnType type, string name, int index)
        {
            Parameters.Add(new FunctionVariableDeclarationParameter(Attribute, Attribute.Scope)
            {
                ReturnType = ReturnType.String,
                Name = name,
                Index = index
            });
            return this;
        }
    }
}