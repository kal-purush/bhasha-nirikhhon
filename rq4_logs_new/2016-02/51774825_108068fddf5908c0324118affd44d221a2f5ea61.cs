using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SKYPE4COMLib;
using System.Threading;
using System.IO;
using System.Net;
namespace BasicSkypeBot
{
    class Program
    {
        internal class ConsoleSpinner
        {
            private int _currentAnimationFrame;
            public ConsoleSpinner()
            {
                SpinnerAnimationFrames = new[]
                                     {
                                         @"[-]",
                                         @"[\]",
                                         @"[|]",
                                         @"[/]"
                                     };
            }
            public string[] SpinnerAnimationFrames
            {
                get;
                set;
            }
            public void UpdateProgress()
            {
                var originalX = Console.CursorLeft;
                var originalY = Console.CursorTop;
                Console.Write(SpinnerAnimationFrames[_currentAnimationFrame]);
                _currentAnimationFrame++;
                if (_currentAnimationFrame == SpinnerAnimationFrames.Length)
                {
                    _currentAnimationFrame = 0;
                }
                Console.SetCursorPosition(originalX, originalY);
            }
        }
        public const string botName = "";
        static string triggerString = "!";

        static Skype skypeVar = new Skype();
        const string botTitle = botName;
        static int totalMessages = 0;
        static void Main(string[] args)
        {
            var titleFinal = "";
            for (int i = 0; i < (botTitle + " [0]").Length; i++)
            {
                titleFinal += (botTitle + " [0]")[i];
                Console.Title = titleFinal;
                Thread.Sleep(75);
            }
            string tempID = ((Path.GetRandomFileName()).Replace(".", "")).ToUpper();
            Console.Clear();
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("────────────────────────────────────────────────────────────────────────────────");
            try
            {
                skypeVar.Attach(7, false);
                skypeVar.MessageStatus += new _ISkypeEvents_MessageStatusEventHandler(skypeVar_MessageStatus);
                skypeVar.UserAuthorizationRequestReceived += new _ISkypeEvents_UserAuthorizationRequestReceivedEventHandler(skypeVar_UserAuthorizationRequestReceived);
                try
                {
                    consoleMessage("Detected [" + skypeVar.Friends.Count + "] contacts", tempID);
                    consoleMessage("Bot hosted on [" + skypeVar.CurrentUserHandle + "]", tempID);
                    consoleMessage("Connceted to Skype.exe", tempID);
                }
                catch
                {
                    consoleError("Contacts cannot be loaded", tempID);
                    consoleError("Unable to detect username", tempID);
                    consoleError("Cannot connect to Skype.exe", tempID);
                }
            }
            catch
            {
                consoleError("Failed to attach " + botName + " to Skype", tempID);
            }
            Console.WriteLine(Environment.NewLine + "────────────────────────────────────────────────────────────────────────────────");
            var s = new ConsoleSpinner();
            while (true)
            {
                Thread.Sleep(150);
                s.UpdateProgress();
            }
        }
        public static string dateTime()
        {
            string returnVal = "[" + DateTime.Now.ToString("hh:mm:ss") + "]";
            return returnVal;
        }
        static void skypeVar_UserAuthorizationRequestReceived(User pUser)
        {
            new Thread(() =>
            {
                try
                {
                    string tempID = ((Path.GetRandomFileName()).Replace(".", "")).ToUpper();
                    if (!(pUser.IsAuthorized))
                    {
                        consoleAdd(pUser.Handle, tempID);
                        Thread.Sleep(250);
                        skypeVar.SendMessage(pUser.Handle, "<auto accept message>");
                        consoleReply("<auto accept message>", tempID);
                    }
                    skypeVar.Friends.Add(pUser);
                    pUser.IsAuthorized = true;
                }
                catch
                {
                }
            }).Start();
            Thread.Sleep(1);
        }
        static void skypeVar_MessageStatus(ChatMessage pMessage, TChatMessageStatus Status)
        {
            if (true)
            {
                string[] cmd = pMessage.Body.Split(' ');
                WebClient client = new WebClient();
                client.Headers["User-Agent"] = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36";
                new Thread(() =>
                {
                    string tempID = ((Path.GetRandomFileName()).Replace(".", "")).ToUpper();
                    try
                    {
                        if (true) //edit for bans and shit.
                        {
                            if (pMessage.Body.StartsWith(triggerString) && Status == TChatMessageStatus.cmsReceived || pMessage.Body.StartsWith(triggerString) && Status == TChatMessageStatus.cmsSending)
                            {
                                consoleLog(pMessage.Sender.Handle + ": " + pMessage.Body, tempID);
                                totalMessages++;
                                Console.Title = botTitle + " [" + totalMessages.ToString() + "]";
                                cmd[0] = cmd[0].Remove(0, triggerString.Length);
                                IChatMessage rMessage = pMessage.Chat.SendMessage("Processing your command");
                                switch (cmd[0].ToLower())
                                {
                                    case "resolve":
                                        if (true)
                                        {
                                            rMessage.Body = "Resolving...";
                                            if (cmd.Length == 2)
                                            {
                                                rMessage.Body = "Resolving " + cmd[1] + "...";
                                                try
                                                {
                                                    string a = client.DownloadString("http://api.predator.wtf/resolver/?arguments=" + cmd[1]);
                                                    int b = Convert.ToInt16(a.Substring(0, 1));
                                                    rMessage.Body = cmd[1] + ": " + a;
                                                }
                                                catch
                                                {
                                                    rMessage.Body = "Unable to resolve " + cmd[1] + ".";
                                                }
                                            }
                                            else
                                            {
                                                rMessage.Body = "Syntax error. The correct syntax for this command is " + triggerString + "resolve <Skype>";
                                            }
                                            consoleReply(rMessage.Body, tempID);
                                        }
                                        break;
                                    case "database":
                                        if (true)
                                        {
                                            rMessage.Body = "Resolving...";
                                            if (cmd.Length == 2)
                                            {
                                                rMessage.Body = "Resolving " + cmd[1] + "...";
                                                try
                                                {
                                                    string a = client.DownloadString("http://api.predator.wtf/lookup/?arguments=" + cmd[1]);
                                                    int b = Convert.ToInt16(a.Substring(0, 1));
                                                    rMessage.Body = cmd[1] + ": " + a;
                                                }
                                                catch
                                                {
                                                    rMessage.Body = "Unable to find past IPs for " + cmd[1] + ".";
                                                }
                                            }
                                            else
                                            {
                                                rMessage.Body = "Syntax error. The correct syntax for this command is " + triggerString + "database <Skype>";
                                            }
                                            consoleReply(rMessage.Body, tempID);
                                        }
                                        break;
                                    case "geoip":
                                        if (true)
                                        {
                                            rMessage.Body = "Tracing...";
                                            if (cmd.Length == 2)
                                            {
                                                rMessage.Body = "Tracing " + cmd[1] + "...";
                                                try
                                                {
                                                    string a = client.DownloadString("http://api.predator.wtf/geoip/?arguments=" + cmd[1]);
                                                    rMessage.Body = a;
                                                }
                                                catch
                                                {
                                                    rMessage.Body = "Unable to trace " + cmd[1] + ".";
                                                }
                                            }
                                            else
                                            {
                                                rMessage.Body = "Syntax error. The correct syntax for this command is " + triggerString + "geoip <IP>";
                                            }
                                            consoleReply(rMessage.Body, tempID);
                                        }
                                        break;
                                    case "help":
                                        if (true)
                                        {
                                            rMessage.Body = "help commands and shit";
                                            consoleReply(rMessage.Body, tempID);
                                        }
                                        break;
                                    default:
                                        if (true)
                                        {
                                            rMessage.Body = "Unknown command, do " + triggerString + "help for commands.";
                                            consoleReply(rMessage.Body, tempID);
                                        }
                                        break;
                                }
                            }
                        }
                    }
                    catch (Exception error)
                    {
                        consoleError(error.ToString(), tempID);
                    }
                }).Start();
                Thread.Sleep(1);
            }
        }
        static void consoleError(string inputMessage, string ID)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("[{2}]=-={0} Error: {1}", dateTime(), inputMessage, ID);
            Console.ForegroundColor = ConsoleColor.White;
        }
        static void consoleMessage(string inputMessage, string ID)
        {
            Console.ForegroundColor = ConsoleColor.DarkGreen;
            Console.WriteLine("[{2}]=-={0} Success: {1}", dateTime(), inputMessage, ID);
            Console.ForegroundColor = ConsoleColor.White;
        }
        static void consoleLog(string inputMessage, string ID)
        {
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("[{2}]=-={0} Log: {1}", dateTime(), inputMessage, ID);
            Console.ForegroundColor = ConsoleColor.White;
        }
        static void consoleReply(string inputMessage, string ID)
        {
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.WriteLine("[{2}]=-={0}" + Environment.NewLine + "{1}", dateTime(), inputMessage, ID);
            Console.ForegroundColor = ConsoleColor.White;
        }
        static void consoleAdd(string inputMessage, string ID)
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine("[{2}]=-={0} Added: {1}", dateTime(), inputMessage, ID);
            Console.ForegroundColor = ConsoleColor.White;
        }
    }
}