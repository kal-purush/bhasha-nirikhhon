using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Tokenizer.Console
{
    public class LexicalAnalyzer
    {
        private readonly Stream inputStream;

        private char[] separators = {'(', ')', '{', '}', ';', '='};

        private bool processingStringLiteral;

        private readonly List<SyntaxToken> syntaxTokens = new List<SyntaxToken>();

        public List<char> buffer = new List<char>();
        public int bufferPositionStart;

        private int tokenStartPosition;
        public LexicalAnalyzer(Stream inputStream)
        {
            this.inputStream = inputStream;
        }

        public SyntaxToken[] Process()
        {
            while (inputStream.Position < inputStream.Length)
            {
                tokenStartPosition = (int)this.inputStream.Position;

                var token = this.ReadToken();

                if (token != null)
                {
                    this.PushToken(token.Value);
                }
            }

            return this.syntaxTokens.ToArray();
        }

        public SyntaxToken? ReadToken()
        {
            if (this.SkipSpace())
            {
                return null;
            }

            var ch = (char) this.inputStream.ReadByte();

            if (IsChar(ch))
            {
                return this.ReadIdentifier(ch);

            }

            if (IsNum(ch))
            {
                return this.ReadNum(ch);
            }

            switch (ch)
            {
                case '+': return this.ReadRepeat2('+', SyntaxTokenType.Increment, '=', SyntaxTokenType.AssigmentAdd, '+', SyntaxTokenType.Addition);
                case '-': return this.ReadRepeat2('+', SyntaxTokenType.Decrement, '=', SyntaxTokenType.AssigmentSubtract, '+', SyntaxTokenType.Subtraction);
                case '&': return this.ReadRepeat2('&', SyntaxTokenType.LogicalAnd, '=', SyntaxTokenType.BitwiseAnd, '&', SyntaxTokenType.LogicalAnd);
                case '|': return this.ReadRepeat2('|', SyntaxTokenType.LogicalOr, '=', SyntaxTokenType.BitwiseOr, '|', SyntaxTokenType.LogicalOr);
                case '=': return ReadRepeat('=', SyntaxTokenType.Equal, SyntaxTokenType.Assignment);
                case '*': return ReadRepeat('=', SyntaxTokenType.AssigmentMultiplication, SyntaxTokenType.Multiplication);
                case '/': return ReadRepeat('=', SyntaxTokenType.AssigmentDivision, SyntaxTokenType.Division);
                case '"': return this.ReadStringLiteral(ch);
                case '{': return this.MakeToken(SyntaxTokenType.CO);
                case '}': return this.MakeToken(SyntaxTokenType.CC);
                case '(': return this.MakeToken(SyntaxTokenType.PO);
                case ')': return this.MakeToken(SyntaxTokenType.PC);
                case '[': return this.MakeToken(SyntaxTokenType.BO);
                case ']': return this.MakeToken(SyntaxTokenType.BC);
                case '>':
                    if (CompareNext('=')) return MakeToken(SyntaxTokenType.MoreOrEqual);
                    return MakeToken(SyntaxTokenType.More);
                case '<':
                    if (CompareNext('=')) return MakeToken(SyntaxTokenType.LessOrEqual);
                    return MakeToken(SyntaxTokenType.Less);
            }

            return null;
        }

        public SyntaxToken ReadIdentifier(char ch)
        {
            this.buffer.Clear();
            this.buffer.Add(ch);

            while (true)
            {
                ch = this.ReadChar();

                if (IsNumOrChar(ch))
                {
                    this.buffer.Add(ch);

                    continue;
                }

                this.Unread();
                return this.MakeIdentifier();
            }
        }

        public SyntaxToken ReadNum(char ch)
        {
            this.buffer.Clear();
            this.buffer.Add(ch);

            while (true)
            {
                ch = this.ReadChar();

                if (this.IsNum(ch) || ch == '.' || ch == 'e' || ch == 'E')
                {
                    this.buffer.Add(ch);

                    continue;
                }

                this.Unread();
                return this.MakeNumber();
            }
        }
        public SyntaxToken MakeToken(SyntaxTokenType tokenType)  => new SyntaxToken()
        {
            Index = this.syntaxTokens.Count,
            Position = this.tokenStartPosition,
            TokenType = tokenType,
        };

        public SyntaxToken MakeIdentifier() => MakeToken(SyntaxTokenType.Identifier);

        public SyntaxToken MakeNumber() => MakeToken(SyntaxTokenType.Number);

        public char ReadChar() => (char) this.inputStream.ReadByte();

        public SyntaxToken ReadRepeat(char expect, SyntaxTokenType expectType, SyntaxTokenType elsType)
        {
            if (CompareNext(expect))
            {
                return MakeToken(expectType);
            }

            return MakeToken(elsType);
        }

        public SyntaxToken ReadRepeat2(char expect1, SyntaxTokenType type1, char expect2, SyntaxTokenType type2, char els, SyntaxTokenType elsType)
        {
            if (this.CompareNext(expect1))
            {
                return MakeToken(type1);
            }

            if (this.CompareNext(expect2))
            {
                return MakeToken(type2);
            }

            return MakeToken(elsType);
        }

        public SyntaxToken ReadStringLiteral(char ch)
        {
            this.buffer.Clear();
            this.buffer.Add(ch);

            while (true)
            {
                ch = this.ReadChar();

                if (ch == '"');
                {
                    return this.MakeStringLiteralToken();
                }

                this.buffer.Add(ch);
            }
        }

        public SyntaxToken MakeStringLiteralToken() => MakeToken(SyntaxTokenType.String);

        public bool SkipSpace()
        {
            var ch = this.ReadChar();

            if (ch == ' ')
            {
                return true;
            }

            this.Unread();

            return false;
        }

        public bool IsNumOrChar(char ch) => IsChar(ch) || IsNum(ch);

        public bool IsChar(char ch)
        {
            if ((ch is >= 'A' and <= 'Z') || ch == '_')
            {
                return true;
            }

            if (ch is >= 'a' and <= 'z')
            {
                return true;
            }

            return false;

        }

        public bool IsNum(char ch)
        {

            if (ch >= '0' && ch <= '9')
            {
                return true;
            }

            return false;
        }

        private bool CompareNext(char expectedChar)
        {
            var actualChar = this.ReadChar();

            if (actualChar == expectedChar)
            {
                return true;
            }

            this.Unread();
            return false;
        }

        private void Unread()
        {
            this.inputStream.Seek(-1, SeekOrigin.Current);
        }

        private void PushToken(SyntaxToken token) => this.syntaxTokens.Add(token);

        private void PushToken(int position, SyntaxTokenType tokenType)
        {
            var token = new SyntaxToken()
            {
                Index = this.syntaxTokens.Count,
                Position = position,
                TokenType = tokenType
            };

            this.syntaxTokens.Add(token);
        }
    }
}