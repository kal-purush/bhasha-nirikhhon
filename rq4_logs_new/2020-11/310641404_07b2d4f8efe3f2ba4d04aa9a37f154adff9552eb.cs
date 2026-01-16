using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;


public class Guestbook
{
    public static List<Message> _message = new List<Message>();

    // Get messages
    public static void GetMessages()
    {
        // _message = JsonSerializer.Deserialize<Message>(File.ReadAllText("guestbook.json"));
        // System.Console.WriteLine(JsonSerializer.Deserialize<Message[]>(File.ReadAllText("guestbook.json")));
        System.Console.WriteLine(File.ReadAllText("guestbook.json"));
    }

    // Write message
    public static void CreateMessage()
    {
        Console.Clear();

        System.Console.WriteLine("Name?");
        var name = Console.ReadLine();

        System.Console.WriteLine("Message");
        var message = Console.ReadLine();

        _message.Add(new Message() 
        { 
            Name = name, 
            Msg = message 
        });

        string json = JsonSerializer.Serialize(_message);

        // Write JSON to file
        File.WriteAllText("guestbook.json", json);
    }

    // Delete message
    public static void DeleteMessage()
    {
        System.Console.WriteLine("Delete Message");
        Console.ReadLine();
    }
}