using System;

class Crypt
{
    string[] alphabets = {
            "abcdefghijklmnopqrstuvwxyz",
            "абвгдеёжзийклмнопрстуфхцчшщъыьэюя" };
    int lang;

    public String Encode(String frase, String key)
    {
        return GetCodedFrase(frase, key, true);
    }

    public String Decode(String frase, String key)
    {
        return GetCodedFrase(frase, key, false);
    }

    public String GetCodedFrase(String line, String key, bool doEncode)
    {
        String codedLine = "";
        for (int i = 0; i < line.Length; i++)
        {
            char keyChar = key[i % key.Length];
            if (IsValidChar(line[i], keyChar))
            {
                bool isUpperCase = line[i].ToString() == line[i].ToString().ToUpper();
                char newChar = Move(line[i], keyChar, doEncode);
                if (isUpperCase)
                    newChar = newChar.ToString().ToUpper()[0];
                codedLine += newChar;
            }
            else codedLine += line[i];
        }
        return codedLine;
    }

    public char Move(char chr, int moveValue, bool doEncode)
    {
        int newOrder;
        int order = GetAlphabetNumber(chr);
        int alphabetLength = alphabets[lang].Length;

        if (!doEncode)
            moveValue *= -1;

        newOrder = (order + moveValue + alphabetLength) % alphabetLength;
        while (newOrder < 0)
            newOrder += alphabetLength;
        return alphabets[lang][newOrder];
    }

    public char Move(char chr, char keyChar, bool doEncode)
    {
        int moveValue = GetAlphabetNumber(keyChar);
        return Move(chr, moveValue, doEncode);
    }

    private bool IsValidChar(params char[] chrs)
    {
        foreach (char chr in chrs)
            if (GetAlphabetNumber(chr) == -1)
                return false;
        return true;
    }

    private int GetAlphabetNumber(char chr)
    {
        for (int i = 0; i < alphabets.Length; i++)
            for (int j = 0; j < alphabets[i].Length; j++)
                if (alphabets[i][j] == chr || alphabets[i][j] == chr.ToString().ToLower()[0])
                {
                    lang = i;
                    return j;
                }
        lang = -1;
        return -1;
    }


}