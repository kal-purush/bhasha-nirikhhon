// $ANTLR 3.4 D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g 2017-03-08 09:36:17

	package select;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked"})
public class SELECTLexer extends Lexer {
    public static final int EOF=-1;
    public static final int ALL_STAR=4;
    public static final int AND=5;
    public static final int APEX=6;
    public static final int COMMA=7;
    public static final int COMPARE=8;
    public static final int DIGIT=9;
    public static final int END_QUERY=10;
    public static final int FROM=11;
    public static final int ID=12;
    public static final int LETTER=13;
    public static final int LP=14;
    public static final int MORE_RELATION=15;
    public static final int NUMBER=16;
    public static final int OR=17;
    public static final int RP=18;
    public static final int SELECT=19;
    public static final int UNDERSCORE=20;
    public static final int VARCHAR=21;
    public static final int WHERE=22;
    public static final int WS=23;

    // delegates
    // delegators
    public Lexer[] getDelegates() {
        return new Lexer[] {};
    }

    public SELECTLexer() {} 
    public SELECTLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public SELECTLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);
    }
    public String getGrammarFileName() { return "D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g"; }

    // $ANTLR start "LETTER"
    public final void mLETTER() throws RecognitionException {
        try {
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:70:16: ( ( 'A' .. 'Z' ) | ( 'a' .. 'z' ) )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:
            {
            if ( (input.LA(1) >= 'A' && input.LA(1) <= 'Z')||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LETTER"

    // $ANTLR start "DIGIT"
    public final void mDIGIT() throws RecognitionException {
        try {
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:72:15: ( ( '0' .. '9' ) )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:
            {
            if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "DIGIT"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:74:4: ( ( ' ' | '\\t' | '\\n' | '\\r' | '\\f' )+ )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:74:6: ( ' ' | '\\t' | '\\n' | '\\r' | '\\f' )+
            {
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:74:6: ( ' ' | '\\t' | '\\n' | '\\r' | '\\f' )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0 >= '\t' && LA1_0 <= '\n')||(LA1_0 >= '\f' && LA1_0 <= '\r')||LA1_0==' ') ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:
            	    {
            	    if ( (input.LA(1) >= '\t' && input.LA(1) <= '\n')||(input.LA(1) >= '\f' && input.LA(1) <= '\r')||input.LA(1)==' ' ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt1 >= 1 ) break loop1;
                        EarlyExitException eee =
                            new EarlyExitException(1, input);
                        throw eee;
                }
                cnt1++;
            } while (true);


            _channel=HIDDEN;

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "WS"

    // $ANTLR start "SELECT"
    public final void mSELECT() throws RecognitionException {
        try {
            int _type = SELECT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:76:8: ( 'SELECT' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:76:10: 'SELECT'
            {
            match("SELECT"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "SELECT"

    // $ANTLR start "FROM"
    public final void mFROM() throws RecognitionException {
        try {
            int _type = FROM;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:78:6: ( 'FROM' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:78:8: 'FROM'
            {
            match("FROM"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "FROM"

    // $ANTLR start "WHERE"
    public final void mWHERE() throws RecognitionException {
        try {
            int _type = WHERE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:80:7: ( 'WHERE' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:80:9: 'WHERE'
            {
            match("WHERE"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "WHERE"

    // $ANTLR start "COMMA"
    public final void mCOMMA() throws RecognitionException {
        try {
            int _type = COMMA;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:82:6: ( ',' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:82:8: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "COMMA"

    // $ANTLR start "APEX"
    public final void mAPEX() throws RecognitionException {
        try {
            int _type = APEX;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:84:6: ( '\\'' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:84:8: '\\''
            {
            match('\''); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "APEX"

    // $ANTLR start "UNDERSCORE"
    public final void mUNDERSCORE() throws RecognitionException {
        try {
            int _type = UNDERSCORE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:86:12: ( '_' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:86:14: '_'
            {
            match('_'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "UNDERSCORE"

    // $ANTLR start "ALL_STAR"
    public final void mALL_STAR() throws RecognitionException {
        try {
            int _type = ALL_STAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:88:10: ( '`*`' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:88:12: '`*`'
            {
            match("`*`"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "ALL_STAR"

    // $ANTLR start "COMPARE"
    public final void mCOMPARE() throws RecognitionException {
        try {
            int _type = COMPARE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:90:8: ( '=' | '<' | '>' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:
            {
            if ( (input.LA(1) >= '<' && input.LA(1) <= '>') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "COMPARE"

    // $ANTLR start "AND"
    public final void mAND() throws RecognitionException {
        try {
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:92:14: ( 'AND' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:92:16: 'AND'
            {
            match("AND"); 



            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "AND"

    // $ANTLR start "OR"
    public final void mOR() throws RecognitionException {
        try {
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:94:13: ( 'OR' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:94:15: 'OR'
            {
            match("OR"); 



            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "OR"

    // $ANTLR start "MORE_RELATION"
    public final void mMORE_RELATION() throws RecognitionException {
        try {
            int _type = MORE_RELATION;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:96:14: ( ( AND | OR )* )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:96:16: ( AND | OR )*
            {
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:96:16: ( AND | OR )*
            loop2:
            do {
                int alt2=3;
                int LA2_0 = input.LA(1);

                if ( (LA2_0=='A') ) {
                    alt2=1;
                }
                else if ( (LA2_0=='O') ) {
                    alt2=2;
                }


                switch (alt2) {
            	case 1 :
            	    // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:96:17: AND
            	    {
            	    mAND(); 


            	    }
            	    break;
            	case 2 :
            	    // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:96:23: OR
            	    {
            	    mOR(); 


            	    }
            	    break;

            	default :
            	    break loop2;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "MORE_RELATION"

    // $ANTLR start "LP"
    public final void mLP() throws RecognitionException {
        try {
            int _type = LP;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:98:4: ( '(' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:98:6: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LP"

    // $ANTLR start "RP"
    public final void mRP() throws RecognitionException {
        try {
            int _type = RP;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:100:4: ( ')' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:100:6: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "RP"

    // $ANTLR start "END_QUERY"
    public final void mEND_QUERY() throws RecognitionException {
        try {
            int _type = END_QUERY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:102:11: ( ';' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:102:13: ';'
            {
            match(';'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "END_QUERY"

    // $ANTLR start "NUMBER"
    public final void mNUMBER() throws RecognitionException {
        try {
            int _type = NUMBER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:104:8: ( ( DIGIT )+ )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:104:10: ( DIGIT )+
            {
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:104:10: ( DIGIT )+
            int cnt3=0;
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( ((LA3_0 >= '0' && LA3_0 <= '9')) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt3 >= 1 ) break loop3;
                        EarlyExitException eee =
                            new EarlyExitException(3, input);
                        throw eee;
                }
                cnt3++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "NUMBER"

    // $ANTLR start "VARCHAR"
    public final void mVARCHAR() throws RecognitionException {
        try {
            int _type = VARCHAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:106:9: ( APEX ( LETTER | DIGIT | UNDERSCORE | ' ' )+ APEX )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:106:11: APEX ( LETTER | DIGIT | UNDERSCORE | ' ' )+ APEX
            {
            mAPEX(); 


            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:106:16: ( LETTER | DIGIT | UNDERSCORE | ' ' )+
            int cnt4=0;
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0==' '||(LA4_0 >= '0' && LA4_0 <= '9')||(LA4_0 >= 'A' && LA4_0 <= 'Z')||LA4_0=='_'||(LA4_0 >= 'a' && LA4_0 <= 'z')) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:
            	    {
            	    if ( input.LA(1)==' '||(input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt4 >= 1 ) break loop4;
                        EarlyExitException eee =
                            new EarlyExitException(4, input);
                        throw eee;
                }
                cnt4++;
            } while (true);


            mAPEX(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "VARCHAR"

    // $ANTLR start "ID"
    public final void mID() throws RecognitionException {
        try {
            int _type = ID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:108:3: ( '`' LETTER ( LETTER | DIGIT | UNDERSCORE )* '`' )
            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:108:5: '`' LETTER ( LETTER | DIGIT | UNDERSCORE )* '`'
            {
            match('`'); 

            mLETTER(); 


            // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:108:16: ( LETTER | DIGIT | UNDERSCORE )*
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( ((LA5_0 >= '0' && LA5_0 <= '9')||(LA5_0 >= 'A' && LA5_0 <= 'Z')||LA5_0=='_'||(LA5_0 >= 'a' && LA5_0 <= 'z')) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    break loop5;
                }
            } while (true);


            match('`'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "ID"

    public void mTokens() throws RecognitionException {
        // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:8: ( WS | SELECT | FROM | WHERE | COMMA | APEX | UNDERSCORE | ALL_STAR | COMPARE | MORE_RELATION | LP | RP | END_QUERY | NUMBER | VARCHAR | ID )
        int alt6=16;
        switch ( input.LA(1) ) {
        case '\t':
        case '\n':
        case '\f':
        case '\r':
        case ' ':
            {
            alt6=1;
            }
            break;
        case 'S':
            {
            alt6=2;
            }
            break;
        case 'F':
            {
            alt6=3;
            }
            break;
        case 'W':
            {
            alt6=4;
            }
            break;
        case ',':
            {
            alt6=5;
            }
            break;
        case '\'':
            {
            int LA6_6 = input.LA(2);

            if ( (LA6_6==' '||(LA6_6 >= '0' && LA6_6 <= '9')||(LA6_6 >= 'A' && LA6_6 <= 'Z')||LA6_6=='_'||(LA6_6 >= 'a' && LA6_6 <= 'z')) ) {
                alt6=15;
            }
            else {
                alt6=6;
            }
            }
            break;
        case '_':
            {
            alt6=7;
            }
            break;
        case '`':
            {
            int LA6_8 = input.LA(2);

            if ( (LA6_8=='*') ) {
                alt6=8;
            }
            else if ( ((LA6_8 >= 'A' && LA6_8 <= 'Z')||(LA6_8 >= 'a' && LA6_8 <= 'z')) ) {
                alt6=16;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 6, 8, input);

                throw nvae;

            }
            }
            break;
        case '<':
        case '=':
        case '>':
            {
            alt6=9;
            }
            break;
        case '(':
            {
            alt6=11;
            }
            break;
        case ')':
            {
            alt6=12;
            }
            break;
        case ';':
            {
            alt6=13;
            }
            break;
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            {
            alt6=14;
            }
            break;
        default:
            alt6=10;
        }

        switch (alt6) {
            case 1 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:10: WS
                {
                mWS(); 


                }
                break;
            case 2 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:13: SELECT
                {
                mSELECT(); 


                }
                break;
            case 3 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:20: FROM
                {
                mFROM(); 


                }
                break;
            case 4 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:25: WHERE
                {
                mWHERE(); 


                }
                break;
            case 5 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:31: COMMA
                {
                mCOMMA(); 


                }
                break;
            case 6 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:37: APEX
                {
                mAPEX(); 


                }
                break;
            case 7 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:42: UNDERSCORE
                {
                mUNDERSCORE(); 


                }
                break;
            case 8 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:53: ALL_STAR
                {
                mALL_STAR(); 


                }
                break;
            case 9 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:62: COMPARE
                {
                mCOMPARE(); 


                }
                break;
            case 10 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:70: MORE_RELATION
                {
                mMORE_RELATION(); 


                }
                break;
            case 11 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:84: LP
                {
                mLP(); 


                }
                break;
            case 12 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:87: RP
                {
                mRP(); 


                }
                break;
            case 13 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:90: END_QUERY
                {
                mEND_QUERY(); 


                }
                break;
            case 14 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:100: NUMBER
                {
                mNUMBER(); 


                }
                break;
            case 15 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:107: VARCHAR
                {
                mVARCHAR(); 


                }
                break;
            case 16 :
                // D:\\Programming\\Eclipse_workspace_ANTLR\\SELECT_ANTLR\\src\\SELECT.g:1:115: ID
                {
                mID(); 


                }
                break;

        }

    }


 

}