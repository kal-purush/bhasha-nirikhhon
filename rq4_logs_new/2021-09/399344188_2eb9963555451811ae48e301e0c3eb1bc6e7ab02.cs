using System;
using System.Diagnostics;

namespace TaskManager
{
    class Program
    {
        static void Main(string[] args)
        {
            var tasks = Process.GetProcesses();
            int num;
            Console.WriteLine();
            while (true)
            {
                Console.Write("command>> ");
                string command = Console.ReadLine();
                if (command == "exit")
                {
                    break;
                }
                else if (command.ToUpper() == "HELP")
                {
                    Console.WriteLine("exit - выход из приложения");
                    Console.WriteLine("KillById <id процесса> - завершение процесса по Id");
                    Console.WriteLine("KillByName <имя процесса> - завершение процесса по имени");
                    Console.WriteLine("TaskList - вывод списка процессов");
                    Console.WriteLine("ProcessId <имя процесса> - определение id процесса по его имени или части имени (без учета регистра)");
                }

                else if (command.ToLower() == "tasklist")
                {
                    TaskList();
                    Console.WriteLine();
                }

                else if (command.ToLower().StartsWith("processid"))
                {
                    string name = command.Split(' ')[1];
                    ProcessId(tasks, name);
                }

                else if (command.ToLower().StartsWith("killbyname"))
                {
                    if (command.ToLower() == "killbyname")
                    {
                        Console.WriteLine("Отсутствует параметр \n");
                        continue;
                    }
                    string proname = command.Split(' ')[1];
                    //Console.WriteLine(proname);
                    KillByName(tasks, proname);
                }

                else if (command.ToLower().StartsWith("killbyid"))
                {
                    if (command.ToLower() == "killbyid")
                    {
                        Console.WriteLine("Не задан параметр \n");
                        continue;
                    }
                    var com = command.Split(' ');
                    num = -1;
                    if (Int32.TryParse(com[1], out num))
                        num = Convert.ToInt32(com[1]);

                    if (num == 0)
                    {
                        Console.WriteLine("Идентификатор задан неверно или не существует \n");
                    }
                    else
                        KillById(tasks, num);


                }

                else
                {
                    Console.WriteLine($"Команда \"{command}\" не определена \nВведите HELP для вывода списка команд. \n");
                }
            }
        }

        static int TaskList()
        {
            var process_list = Process.GetProcesses();
            Console.WriteLine($"{"Имя процесса",-40}  {"PID"}");
            for (int i = 0; i < process_list.Length; i++)
            {
                Console.WriteLine($"{ process_list[i].ProcessName,-40}  {process_list[i].Id}");
            }
            return 0;
        }

        static int KillById(Process[] tasks, int id)
        {
            for (int i = 0; i < tasks.Length; i++)
            {
                if (tasks[i].Id == id)
                {
                    tasks[i].Kill();
                    Console.WriteLine($"Процесс \"{tasks[i].ProcessName}\" завершен\n");
                    return 0;
                }
            }
            return -1;
        }

        static int ProcessId(Process[] tasks, string name)
        {
            int k = 0;
            for (int i = 0; i < tasks.Length; i++)
            {
                if (tasks[i].ProcessName.ToLower().IndexOf(name.ToLower())>=0)
                {
                    Console.WriteLine($"Id процесса \"{tasks[i].ProcessName}\"   {tasks[i].Id}");
                    k++;
                    
                }
            }
            if (k == 0)
                Console.WriteLine("Процесса с таким именем не существует");

            return 0;
        }

        static int KillByName(Process[] tasks, string name)
        {
            for (int i = 0; i < tasks.Length; i++)
            {
                if (tasks[i].ProcessName == name)
                {
                    tasks[i].Kill();
                    Console.WriteLine($"Процесс \"{tasks[i].ProcessName}\" завершен\n");
                    return 0;
                }
            }
            return -1;
        }

    }
}