using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.VisualBasic.CompilerServices;

namespace Tokenizer.Console
{
    public class SyntaxAnalyzer
    {
        private SyntaxToken[] tokens;

        private int current = 0;

        public SyntaxAnalyzer(SyntaxToken[] tokens)
        {
            this.tokens = tokens;
        }

        public SyntaxNode ReadDeclarationOrStatement()
        {
            var first = this.Peek();

            if (CheckNextToken(SyntaxTokenType.Identifier))
            {
                // Declaration
                this.Unread(2);

                return this.ReadDeclaration();
            }
            else
            {
                this.Unread(1);

                return this.ReadExpression();
            }
        }

        public SyntaxNode ReadCompoundStatement()
        {
            var list = new List<SyntaxNode>();

            while (true)
            {
                if (this.CheckNextToken(SyntaxTokenType.CC))
                {
                    break;
                }

                list.Add(this.ReadDeclarationOrStatement());
            }

            return new CompoundSyntaxNode()
            {
                InnerNodes = list.ToArray(),
            };
        }

        public SyntaxNode ReadExpressionOpt()
        {
            this.CheckNextToken(SyntaxTokenType.PO);

            var condition = this.ReadBooleanExpression();

            this.CheckNextToken(SyntaxTokenType.PC);

            var body = this.ReadStatement();

            return new CompoundSyntaxNode()
            {
                InnerNodes = new SyntaxNode[] {condition, body}
            };
        }

        public SyntaxNode ReadDeclaration()
        {
            // int a = 1;
            // int a = x;
            // int a = x + 1;

            var typeToken = this.Peek();

            if (typeToken.TokenType != SyntaxTokenType.Identifier)
            {
                System.Console.WriteLine("Expected type identifier");
            }

            var nameToken = this.Peek();

            if (nameToken.TokenType != SyntaxTokenType.Identifier)
            {
                System.Console.WriteLine("Expected name identifier");
            }

            var assignmentToken = this.Peek();

            if (assignmentToken.TokenType != SyntaxTokenType.Assignment)
            {
                System.Console.WriteLine("Expected assignment token");
            }

            var value = this.ReadExpression();

            this.CheckSemicolon();

            var variableNode = new LocalVariableSyntaxNode() {Name = nameToken.Value, Type = typeToken.Value};

            return new DeclarationSyntaxNode()
            {
                Variable = variableNode,
                Value = value
            };
        }

        public SyntaxNode ReadForStatement()
        {
            this.CheckNextToken(SyntaxTokenType.PO);

            var init = this.ReadDeclaration();

            this.CheckNextToken(SyntaxTokenType.Semicolon);

            var condition = this.ReadComparison();

            this.CheckNextToken(SyntaxTokenType.Semicolon);

            var step = this.ReadExpression();

            this.CheckNextToken(SyntaxTokenType.PC);

            var body = this.ReadStatement();

            return new CompoundSyntaxNode()
            {
                InnerNodes = new SyntaxNode[] {init, condition, step, body}
            };
        }

        public SyntaxNode ReadWhileStatement()
        {
            this.CheckNextToken(SyntaxTokenType.PO);

            var condition = this.ReadComparison();

            this.CheckNextToken(SyntaxTokenType.PC);

            var body = this.ReadStatement();

            return new CompoundSyntaxNode()
            {
                InnerNodes = new SyntaxNode[] { condition, body }
            };
        }

        public SyntaxNode ReadStatement()
        {
            var token = this.Peek();

            switch (token.TokenType)
            {
                case SyntaxTokenType.CO: return this.ReadCompoundStatement();
                case SyntaxTokenType.If: return this.ReadIf();
                case SyntaxTokenType.For: return this.ReadForStatement();
                case SyntaxTokenType.While: return this.ReadWhileStatement();
            }

            System.Console.WriteLine("Unexpected transition detected");

            return null;
        }

        public SyntaxNode ReadComparison()
        {
            var toCompare = this.Peek();

            if (toCompare.TokenType != SyntaxTokenType.Identifier)
            {
                System.Console.WriteLine("Expected identifier token");
            }

            var @operator = this.Peek();

            if (!IsComparisonOperator(@operator.TokenType))
            {
                System.Console.WriteLine("Expected comparison token");
            }

            var value = this.Peek();

            if (!(value.TokenType == SyntaxTokenType.Identifier || value.TokenType == SyntaxTokenType.Number))
            {
                System.Console.WriteLine("Expected identifier token");
            }

            return new BinaryOperatorSyntaxNode()
            {
                Type = (int) @operator.TokenType,
                Left = toCompare,
                Right = value,
            };
        }

        public bool IsComparisonOperator(SyntaxTokenType tokenType)
        {
            return tokenType == SyntaxTokenType.Less ||
                   tokenType == SyntaxTokenType.LessOrEqual ||
                   tokenType == SyntaxTokenType.More ||
                   tokenType == SyntaxTokenType.MoreOrEqual ||
                   tokenType == SyntaxTokenType.Equal;
        }
        public ConditionalSyntaxNode ReadIf()
        {
            this.CheckNextToken(SyntaxTokenType.PO);
            var condition = this.ReadExpression();
            this.CheckNextToken(SyntaxTokenType.PC);
            var innerStatement = this.ReadStatement();

            if (!this.CheckNextToken(SyntaxTokenType.Else))
            {
                return new ConditionalSyntaxNode()
                {
                    ConditionBlock = condition,
                    ThenBlock = innerStatement,
                    ElseBlock = null
                };
            }

            var elseStatement = this.ReadStatement();

            return new ConditionalSyntaxNode()
            {
                ConditionBlock = condition,
                ThenBlock = innerStatement,
                ElseBlock = elseStatement
            };
        }

        public SyntaxNode ReadBooleanExpression()
        {
            var condition = this.ReadExpression();

            return new BooleanSyntaxNode() {Condition = condition,};
        }

        public SyntaxNode ReadExpression()
        {
            Stack<SyntaxToken> tokenStack = new Stack<SyntaxToken>();

            while (true)
            {
                var token = this.Peek();

                if (token.TokenType == SyntaxTokenType.Semicolon)
                {
                    break;
                }

                tokenStack.Push(token);
            }

            SyntaxNode previousNode = null;
            SyntaxToken previousToken = null;

            while (tokenStack.Count != 0)
            {
                var token = tokenStack.Pop();

                if (previousToken == null)
                {
                    previousToken = token;

                    continue;
                }

                if (IsUnaryOperator(token.TokenType))
                {
                    var node = new UnaryOperatorSyntaxNode();

                    if (previousNode == null)
                    {
                        previousNode = node;
                    }
                    else
                    {
                        var operation = tokenStack.Pop();

                        previousNode = new BinaryOperatorSyntaxNode()
                        {
                            Left = previousNode,
                            Right = node,
                            Type = (int) operation.TokenType
                        };
                    }
                }
                else
                {
                    var operand = tokenStack.Pop();

                    if (previousNode == null)
                    {
                        previousNode = new BinaryOperatorSyntaxNode()
                        {
                            Left = previousToken,
                            Right = operand,
                            Type = (int)token.TokenType
                        };
                    }
                    else
                    {
                        previousNode = new BinaryOperatorSyntaxNode()
                        {
                            Left = previousNode,
                            Right = operand,
                            Type = (int)token.TokenType
                        };
                    }
                   
                }
            }

            return previousNode;
        }

        public bool IsUnaryOperator(SyntaxTokenType type)
        {
            return type == SyntaxTokenType.Increment || type == SyntaxTokenType.Decrement;
        }

        public Node ReadLogicalOrExpression()
        {
            return null;
        }

        public int GetCompoundAssigmentOp(SyntaxToken token)
        {
            return token.TokenType switch
            {
                SyntaxTokenType.AssigmentAdd => '+',
                SyntaxTokenType.AssigmentSubtract => '-',
                SyntaxTokenType.AssigmentMultiplication => '*',
                SyntaxTokenType.AssigmentDivision => '/',
                _ => 0
            };
        }
        public Node ReadAssigmentExpression()
        {
            var node = this.ReadLogicalOrExpression();
            var token = this.Peek();

            if (token == null)
            {
                return node;
            }
            var compoundAssignmentOp = GetCompoundAssigmentOp(token);

            if (compoundAssignmentOp != 0)
            {

            }
            return null;
        }

        public bool IsType(SyntaxToken token)
        {
            if (token.TokenType == SyntaxTokenType.Identifier)
            {

            }

            return true;
        }

        public bool CheckNextToken(SyntaxTokenType type)
        {
            if (this.tokens[this.current].TokenType == type)
            {
                this.current++;
                
                return true;
            }

            return false;
        }

        public void CheckSemicolon()
        {
            if (!CheckNextToken(SyntaxTokenType.Semicolon))
            {
                System.Console.WriteLine("Expected semicolon token");
            }
        }

        public void Unread(int count = 1) => this.current -= count;

        public SyntaxToken Peek() => this.tokens[this.current++];
    }
}