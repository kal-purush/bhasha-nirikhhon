using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;

namespace Client
{
    public class Authorization
    {
        static User? AuthUser { get; set; }
        private static Options ReadOperation()
        {
            Console.Write("""
                    1. Login 
                    2. Registration
                    3. Exit
                    Choose an option:
                    """);
            string? input = Console.ReadLine();
            bool successfulParsing = Enum.TryParse<Options>(input, out Options result);
            if (!successfulParsing)
            {
                Console.Clear();
                Console.WriteLine("You enter wrong number");
                Console.WriteLine();
                return ReadOperation();
            }
            else if (result < Options.Login || result > Options.Exit)
            {
                Console.Clear();
                Console.WriteLine($"Please enter number from 1 to 3");
                Console.WriteLine();
                return ReadOperation();
            }
            return result;
        }

        private static string ReadString(string message, bool isEmail = false)
        {
            Console.Write(message);
            string? input = Console.ReadLine();
            string regex = @"^[^@\s]+@[^@\s]+\.(com|net|org|gov)$";

            if (String.IsNullOrEmpty(input))
            {
                Console.WriteLine("Field can not be null or empty");
                return ReadString(message);
            } else if (isEmail && !Regex.IsMatch(input, regex, RegexOptions.IgnoreCase))
            {
                Console.WriteLine("Email is invalid!");
                return ReadString(message);
            }
            return input;

        }

        private static Task Login(StreamWriter writer)
        {
            return Task.Run(async () =>
            {
               
                string? email = ReadString("Enter email: ", true);
                string? password = ReadString("Enter password: ");

                await Write(writer, $"Login#Email={email}&Password={password}");
            });
        }

        private static Task Registration(StreamWriter writer)
        {
            return Task.Run(() =>
            {
                string? name = ReadString("Enter name: ");
                string? email = ReadString("Enter email: ", true);
                string? password = ReadString("Enter password: ");

                User user = new User(name, email, password);

                return Write(writer, user.ToString());
            });
        }

        public static Task Process(NetworkStream stream)
        {
            return Task.Run(async () =>
            {
                var reader = new StreamReader(stream);
                var writer = new StreamWriter(stream);
                while (true)
                {
                    Console.Clear();
                    Options input = ReadOperation();

                    switch (input)
                    {
                        case Options.Login:
                            await Login(writer);
                            bool loginError = false;
                            var endPoint = stream.Socket.LocalEndPoint;
                            while (!loginError)
                            {
                                var msg = await reader.ReadLineAsync();
                                if (!string.IsNullOrEmpty(msg))
                                {
                                    var splittedMsg = msg.Split("#");
                                    if (splittedMsg[0] == endPoint?.ToString() && splittedMsg[1].IndexOf("Auth") != -1)
                                    {
                                        var data = splittedMsg[1].Split("&");
                                        var auth = data[0].Split("=");
                                        if (auth[1] == "False")
                                        {
                                            Console.WriteLine("Login error!");
                                            Console.ReadLine();
                                            loginError = true;
                                        }
                                        else
                                        {
                                            Console.Clear();
                                            Console.WriteLine("Login success!");

                                            CreateAuthUser(splittedMsg[2]);

                                            var readTask = Read(stream, reader);
                                            var writeTask = Write(writer);
                                            Task.WaitAll(readTask, writeTask);
                                        }
                                    }
                                }
                            }
                            break;
                        case Options.Registration:
                            await Registration(writer);
                            break;
                        case Options.Exit:
                            Console.WriteLine("Exiting program");
                            return;
                        default:
                            Console.WriteLine("Invalid option");
                            Console.ReadLine();
                            break;
                    }
                    Console.Clear();
                }
            });
        }

        static void CreateAuthUser(string msg)
        {
            var fieldsPairs = msg.Split("&");
            string name = fieldsPairs[0].Split("=")[1];
            string email = fieldsPairs[1].Split("=")[1];
            string password = fieldsPairs[2].Split("=")[1];
            AuthUser = new User(name, email, password);
        }

        static Task Write(StreamWriter writer, string message = "")
        {
            return Task.Run(async () =>
            {
                if (string.IsNullOrEmpty(message))
                {
                    while (true)
                    {
                        var text = Console.ReadLine();
                        await writer.WriteLineAsync(text);
                        await writer.FlushAsync();
                    }
                } else
                {
                    await writer.WriteLineAsync(message);
                    await writer.FlushAsync();
                }
            });
        }

        static Task Read(NetworkStream stream, StreamReader reader)
        {
            return Task.Run(async () =>
            {
                var endPoint = stream.Socket.LocalEndPoint;
                while (true)
                {
                    var msg = await reader.ReadLineAsync();
                    if (!string.IsNullOrEmpty(msg))
                    {
                        var splittedMsg = msg.Split("#");
                        if (splittedMsg[0] != endPoint?.ToString())
                        {
                            if (splittedMsg[1].StartsWith("@"))
                            {
                                string pattern = @"@([^:\s]+)";
                                Match match = Regex.Match(splittedMsg[1], pattern);
                                if (match.Success)
                                {
                                    string name = match.Groups[1].Value;
                                    if (name == AuthUser?.Name)
                                    {
                                        Console.WriteLine(msg);
                                    }
                                }
                            }
                            else
                            {
                                Console.WriteLine(msg);
                            }
                        }
                    }
                }
            });
        }
    }
}