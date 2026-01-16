using Microsoft.Win32.TaskScheduler;
using System;

class BackupScheduling
{
    public void Asignar ()
    {
        // Crear una nueva tarea
        using (TaskService ts = new TaskService())
        {
            TaskDefinition td = ts.NewTask();
            td.RegistrationInfo.Description = "Ejecutar comando CMD";

        // Configurar el disparador (trigger)
        DailyTrigger dailyTrigger = new DailyTrigger { DaysInterval = 1 };
            dailyTrigger.StartBoundary = DateTime.Today + TimeSpan.FromHours(21);
        td.Triggers.Add(dailyTrigger);

        // Configurar la acción
        td.Actions.Add(new ExecAction("cmd.exe", "mysqldump -u root -p proyecto > \\\\ALEX\\Users\\Public\\respaldo\\backup.sql\r\n", "C:\\Program Files\\MariaDB 11.3\\bin"));

            // Registrar la tarea en el programador de tareas
            ts.RootFolder.RegisterTaskDefinition(@"MiTareaCMD", td);
        }

        Console.WriteLine("Tarea programada exitosamente.");
    }
}