using BabyCsharpProject.LexerParserClases.Syntax;
using System;
using System.Collections.Generic;
using System.Text;

namespace BabyCsharpProject.LexerParserClases.Binding
{
    internal sealed class Binder
    {
        private readonly List<string> _diagnostics = new List<string>();
        public IEnumerable<string> Diagnostics => _diagnostics;
        public BoundExpress BindExpression(EvalSyntax syntax)
        {
            switch (syntax.Type)
            {
                case TypeOfSyntax.EvalNumber:
                    return BindLiteralEspress((NumEvalSyntx)syntax);
                case TypeOfSyntax.UnaryBinaryEvaluator:
                    return BindUnaryEspress((UnaryEvalSyntxBinary)syntax);
                case TypeOfSyntax.BinaryEvaluator:
                    return BindBinaryEspress((EvalSyntxBinary)syntax);
                default:
                    throw new Exception($"Unexpected syntax {syntax.Type}");
            }
        }
        private BoundExpress BindLiteralEspress(NumEvalSyntx syntax)
        {
            
            var value = syntax.Value  ?? 0;
            return new BoundLiteralExpress(value);
        }
        private BoundExpress BindBinaryEspress(EvalSyntxBinary syntax)
        {
            var boundLeft = BindExpression(syntax.LeftSyn);
            var boundRight = BindExpression(syntax.RightSyn);
            var boundOppType = BindBinaryOppType(syntax.Evaluator.Type, boundLeft.Type, boundRight.Type);
            if (boundOppType == null)
            {
                _diagnostics.Add($"Binary oprerator '{syntax.Evaluator.TextString}' is not defined for type {boundLeft.Type} and {boundRight.Type}");
                return boundLeft;
            }
            return new BoundBinaryExpress(boundLeft, boundOppType.Value, boundRight);
        }



        private BoundExpress BindUnaryEspress(UnaryEvalSyntxBinary syntax)
        {
            var boundOpp = BindExpression(syntax.Opperand);
            var boundOppType = BindUnaryOppType(syntax.OperatorToken.Type, boundOpp.Type);
            if(boundOppType == null)
            {
                _diagnostics.Add($"Unary oprerator '{syntax.OperatorToken.TextString}' is not defined for type {boundOpp.Type}");
                return boundOpp;
            }
            return new BoundUnaryExpress(boundOppType.Value, boundOpp);
        }

        private BoundUnaryOppType? BindUnaryOppType(TypeOfSyntax type, Type opperandType)
        {
            if(opperandType == typeof(int)){
                 switch (type)
                 {
                    case TypeOfSyntax.SumToken:
                        return BoundUnaryOppType.Identity;
                    case TypeOfSyntax.SubToken:
                        return BoundUnaryOppType.Negation;
                   
                 }
              
            }
             if(opperandType == typeof(bool)){
                 switch (type)
                 {
                    case TypeOfSyntax.LogicANDToken:
                        return BoundUnaryOppType.LogicNegation;
                  
                 }
              
            }

             return null;
        }
        private BoundBinaryOppType? BindBinaryOppType(TypeOfSyntax type, Type leftType, Type rightType)
        {
            if (leftType == typeof(int) || rightType == typeof(int))
            {
                 switch (type)
                 {
                    case TypeOfSyntax.SumToken:
                        return BoundBinaryOppType.Add;
                    case TypeOfSyntax.SubToken:
                        return BoundBinaryOppType.Sub;
                    case TypeOfSyntax.MultToken:
                        return BoundBinaryOppType.Mult;
                    case TypeOfSyntax.DivToken:
                        return BoundBinaryOppType.Div;
                 }
               
            }
             if (leftType == typeof(bool) || rightType == typeof(bool))
            {
                 switch (type)
                 {
                    case TypeOfSyntax.LogicANDToken:
                        return BoundBinaryOppType.OppAND;
                    case TypeOfSyntax.LogicORToken:
                        return BoundBinaryOppType.OppOR;
                 }
               
            }
             return null;
            
        }

        
    }
}