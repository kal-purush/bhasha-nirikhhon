/* lexer.ts  */

module TSC
	{
	export class Lexer {
		public static lex() {
		    {
		        // Grab the "raw" source code.
		        var sourceCode = (<HTMLInputElement>document.getElementById("taSourceCode")).value;
		        // Trim the leading and trailing spaces.
		        sourceCode = TSC.Utils.trim(sourceCode);

                var tokenStream = new Array<Token>();
                var tokenCounter = 0;

                var lineSplitCode = sourceCode.match(/[^\r\n]+/g);
                for (var i = 0; i < lineSplitCode.length; i++) {
                    lineSplitCode[i] = TSC.Utils.trim(lineSplitCode[i]);
                }
                var currentLine = lineSplitCode[0];

                for (var j = 0; j < lineSplitCode.length; j++) {

                    // Regex Match For:                 ID      Integers           Strings     +    EQ   NEQ  =   TypeI TypeS    TypeB     BoolF   BoolT  Parens  IF   WHILE   PRINT   Bracks  Space
                    var tokenMatch = currentLine.match(/([a-z])|(0|(^[1-9][0-9]*))|(^"[^"]*$")|(\+)|(==)|(!=)|(=)|(int)|(string)|(boolean)|(false)|(true)|(\(|\))|(if)|(while)|(print)|(\{|\})|(\S)/g);
                    while (currentLine != "") {
                        tokenStream[tokenCounter] = this.generateToken(tokenMatch);//new Token(tokenMatch);
                        tokenCounter++;

                        // TODO: Check if regex match removes previous token data, else shift currentLine string and match again

                        tokenMatch = currentLine.match(/([a-z])|(0|(^[1-9][0-9]*))|(^"[^"]*$")|(\+)|(==)|(!=)|(=)|(int)|(string)|(boolean)|(false)|(true)|(\(|\))|(if)|(while)|(print)|(\{|\})|(\S)/g);
                    }

                }

		        return tokenStream;
		    }
		}

        public generateToken(tokenVal: string): TSC.Token {



            return new Token();
        }

	}
	}