using Lexer.Forms.Enums;
using Lexer.Model;
using System.Collections.Generic;

namespace Lexer.Controller
{
    public class LexerController
    {

        public IEnumerable<TokenViewModel> FindToken(ToValidateViewModel toValidate)
        {
            List<TokenViewModel> tokenList = null;
            return tokenList;
        }

        private int FirstCharacterPosition(string text)
        {
            int position = 0;
            while (char.IsWhiteSpace(text[position]))
            {
                position++;
            }
            return position;
        }

        private TokenViewModel ToToken(string textToValidate)
        {
            TokenViewModel token = null;
            token.TypeToken = Token.IDENTIFIER;
            token.Text = textToValidate;
            return token;
        }

    }
}