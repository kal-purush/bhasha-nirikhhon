using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TaskDudes.Models;
using TaskDudes.Services;

namespace TaskDudes.Controllers
{
    public class SettingsController
    {
        TaskMatesContext context = new TaskMatesContext();

        public Settings GetUserSettings(string id) 
        { 
            return context.Settings.Find(id); 
        }

        public async void AddNewSettingsAsync(Settings settings)
        {
            context.Settings.Add(settings);
            await context.SaveChangesAsync();
        }

        public async void UpdateSettingsAsync(Settings settings)
        {
            context.Settings.Update(settings);
            await context.SaveChangesAsync();
        }

        public async void RemoveSettingsAsync(Settings settings)
        {
            context.Settings.Remove(settings);
            await context.SaveChangesAsync();
        }
    }
}