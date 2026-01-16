using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using UnityEngine;

public class CommandProcessor : MonoBehaviour
{
    private List<String> commandComponents = new List<String>();
    private readonly string[] validCommands = { "echo", "help", "prof" };

    public void processCommand(string command)
    {
        commandComponents.Clear();
        int point = 0;

        for (int i = 0; i < command.Length; i++)
        {
            if (command.Substring(i, 1).Equals(" "))
            {
                commandComponents.Add(command.Substring(point, i - point));

                for (; i < command.Length; i++)
                {
                    if (command.Substring(i, 1).Equals(" ") == false)
                    {
                        break;
                    }
                }

                point = i;
            }
        }

        if (command.Substring(point, command.Length - point).Replace(" ", String.Empty).Length > 0)
        {
            commandComponents.Add(command.Substring(point, command.Length - point));
        }

        //Make the root command lowercase
        commandComponents[0] = commandComponents[0].ToLower();

        bool validCommand = false;

        foreach(string s in validCommands)
        {
            if (commandComponents[0].Equals(s))
            {
                validCommand = true;
                break;
            }
        }

        if (validCommand == false)
        {
            invalidCommand();
            return;
        }

        //Check the root word
        switch (commandComponents[0].ToString())
        {
            case "echo":
                echo(command);
                break;
            case "help":
                help();
                break;
            case "prof":
                profiler();
                break;
            default:
                Debug.LogError("Made it to the default case when processing the last command.");
                break;
        }
    }

    private void invalidCommand()
    {
        Debug.LogWarning("The base command <i>" + commandComponents[0] + "</i> is not a valid command. Type \"help\" for a list of commands.");
    }

    private void echo(String command)
    {
        int repeats = 0;

        if (commandComponents.Count == 1)
        {
            Debug.LogWarning("Missing second argument in structure for echo: echo {number of echos|message} [message]");
        }
        else if (commandComponents.Count > 2 && Int32.TryParse(commandComponents[1].ToString(), out repeats) == true)
        {
            Debug.Log("Echoing <i>" + command.Substring(6 + (int)(Math.Floor(Math.Log10(repeats) + 1)), command.Length - 6 - (int)(Math.Floor(Math.Log10(repeats) + 1))) + "</i> " + repeats 
                + ((repeats == 1) ? " time..." : " times..."));

            for (int i = 0; i < repeats; i++)
            {
                Debug.Log("@echo: <i>" + command.Substring(6 + (int)(Math.Floor(Math.Log10(repeats) + 1)), command.Length - 6 - (int)(Math.Floor(Math.Log10(repeats) + 1))) + "</i>");
            }
        }
        else
        {
            Debug.Log("@echo: <i>" + command.Substring(5, command.Length - 5) + "</i>");
        }
    }

    private void profiler()
    {
        //Make the subroot lowercase
        commandComponents[1] = commandComponents[1].ToLower();

        if (commandComponents.Count == 1)
        {
            Debug.LogWarning("Missing second argument in structure for profiler: prof {all/1|min/2|off/0}");
        }
        else if (commandComponents.Count > 2 && (commandComponents[1].Equals("all") || commandComponents[1].Equals("1") || commandComponents[1].Equals("min")
            || commandComponents[1].Equals("2") || commandComponents[1].Equals("off") || commandComponents[1].Equals("0")))
        {
            switch (commandComponents[1].ToString())
            {
                case "all":
                case "1":
                    break;
                case "min":
                case "2":
                    break;
                case "off":
                case "0":
                    break;
            }
        }
        else
        {
            Debug.LogWarning("Invalid second argument in structure for profiler: prof {all/1|min/2|off/0}");
        }
    }

    private void help()
    {
        Debug.Log("Valid commands: ");

        for (int i = 0; i < validCommands.Length; i += 2)
        {
            if (i + 2 <= validCommands.Length)
            {
                Debug.Log("\t" + validCommands[i] + "\t\t\t" + validCommands[i + 1]);
            }
        }
    }
}