package roo2;

public abstract class Complement implements Cipher{

    char[] alphabet;


    public Complement(String inputAlphabet){
        alphabet = new char[inputAlphabet.length()];
        inputAlphabet.getChars(0,inputAlphabet.length(), alphabet, 0);
    }

    protected abstract char cipherChar( char inputChar);

    protected abstract char decipherChar( char inputChar);


    public String cipher(String inputText){
        char[] result = new char[inputText.length()] ;
        inputText.getChars(0, inputText.length(), result, 0);

        for (int idx=0; idx < result.length; idx++)
            result[idx]=cipherChar(result[idx]);
        return new String(result);
    };

    public String decipher(String inputText){
        char[] result = new char[inputText.length()] ;
        inputText.getChars(0, inputText.length(), result, 0);

        for (int idx=0; idx < result.length; idx++)
            result[idx]=decipherChar(result[idx]);
        return new String(result);
    };

    public char offset (int idx, char inputChar, int valor){
        char result;
        int offset;
        if(idx < 0){
            result= inputChar;
        }
        else{ offset = idx + valor;
            if(offset<alphabet.length){
                result= alphabet[offset];
            }
            else{
                result= alphabet[offset - alphabet.length];
            }
        }
        return result;
    }
    public char deoffset (int idx, char inputChar, int valor){
        int offset;
        char result;

        if(idx <0){
            result =inputChar;
        }
        else{
            offset = idx - valor;

            if(offset>=0){
                result= alphabet[offset];
            }
            else{
                result= alphabet[ alphabet.length + offset];
            }
        }
        return result;
    }
}
