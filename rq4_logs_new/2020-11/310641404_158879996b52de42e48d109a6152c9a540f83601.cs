using System;
using System.IO;
using System.Text.Json;
using System.Collections.Generic;

public class Utilities
{
    public static bool IsValidString(string str)
    {
        if (str == null | str == "")
        {
            return false;
        }
        return true;
    }



    public static void JsonSerializeMessages(List<Message> _messages)
    {
        string json;

        json = JsonSerializer.Serialize(_messages);
        File.WriteAllText("guestbook.json", json); 
    }



    public static bool IsValidInt(char key, int messageCount)
    {
        Int16 inputNumber;
        if (Int16.TryParse(key.ToString(), out inputNumber))
        {
            if (inputNumber >= 0 && inputNumber < messageCount)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        else {
            return false;
        }
    }
}