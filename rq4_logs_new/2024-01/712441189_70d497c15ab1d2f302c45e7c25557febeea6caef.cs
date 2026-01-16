using System.IO.MemoryMappedFiles;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Drawing;
using System.Runtime.InteropServices;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Client
{
    public class Authorization
    {
        private Options ReadOperation()
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

        private Task<bool> Login()
        {
            return Task.Run(() =>
            {
                Console.Write("Enter email: ");
                string? email = Console.ReadLine();
                Console.Write("Enter password: ");
                string? password = Console.ReadLine();

                return true;
            });
        }

        private Task Registration()
        {
            return Task.Run(async () =>
            {
                Console.Write("Enter name: ");
                string? name = Console.ReadLine();
                Console.Write("Enter email: ");
                string? email = Console.ReadLine();
                Console.Write("Enter password: ");
                string? password = Console.ReadLine();

                User user = new User(name, email, password);

                await SerializeToMemoryMappedFile(user);
                List<User> userList = await DeserializeFromMemoryMappedFile();
                Console.WriteLine(userList.Count);
                Console.ReadLine();
            });
        }

        private Task SerializeToMemoryMappedFile(User user)
        {
            return Task.Run(() =>
            {
                try
                {
                    string json = JsonSerializer.Serialize(new List<User>() { user });

                    byte[] jsonBytes = System.Text.Encoding.UTF8.GetBytes(json);

                    using (MemoryMappedFile mmf = MemoryMappedFile.CreateOrOpen("UserListMappedFile", jsonBytes.Length))
                    {
                        using (MemoryMappedViewAccessor accessor = mmf.CreateViewAccessor())
                        {
                            accessor.WriteArray(0, jsonBytes, 0, jsonBytes.Length);
                        }
                    }
                    Console.WriteLine("Data written to memory-mapped file.");
                } catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            });
        }

        private Task<List<User>?> DeserializeFromMemoryMappedFile()
        {
            return Task.Run(() =>
            {
                try
                {
                    using (MemoryMappedFile mmf = MemoryMappedFile.OpenExisting("UserListMappedFile"))
                    {
                        using (MemoryMappedViewAccessor accessor = mmf.CreateViewAccessor())
                        {
                            byte[] jsonBytes = new byte[accessor.Capacity];
                            accessor.ReadArray(0, jsonBytes, 0, jsonBytes.Length);

                            string json = System.Text.Encoding.UTF8.GetString(jsonBytes);
                            return JsonSerializer.Deserialize<List<User>>(json);
                        }
                    }
                } catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return new List<User>();
                }
            });
        }

        public Task Process(Action startChat)
        {
            return Task.Run(async () =>
            {
                while (true)
                {
                    Console.Clear();
                    Options input = ReadOperation();

                    switch (input)
                    {
                        case Options.Login:
                            bool loginSuccess = await Login();
                           
                            if (loginSuccess)
                            {
                                Console.WriteLine("Login success!");
                                startChat();
                            }
                            else
                            {
                                Console.WriteLine("Login error!");
                                Console.ReadLine();
                            }
                            break;
                        case Options.Registration:
                            await Registration();
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
    }
}