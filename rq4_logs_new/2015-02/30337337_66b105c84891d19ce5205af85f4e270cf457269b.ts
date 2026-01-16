module TSC {
    
    export class TokenInvalid extends Token {

        public constructor(tokenVal: string, lineNum: number) {

            super(tokenVal, lineNum);

        }

    }

}