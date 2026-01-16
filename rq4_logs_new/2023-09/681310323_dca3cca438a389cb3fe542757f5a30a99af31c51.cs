using Discord;
using DiscordBot.Utility;
using Microsoft.Extensions.DependencyInjection;
using PluginTest.Enums;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace DiscordBot.Managers
{
    //TO DO: Make a backup only when files from current and last backup are different. 
    public class BackupManager
    {
        private readonly int MaxBackups;
        private readonly int MaxOldBackups;
        private readonly string ModuleName = "Backup Manager";
        private readonly string BackupFolderPath;
        private bool started = false;
        private List<string> FileList;
        private Logger Logger;

        public BackupManager(int updateInterval, string backupFolderPath, IServiceProvider serviceProvider, int maxBackups = 30, int maxOldBackups = 30)
        {
            MaxBackups = maxBackups;
            MaxOldBackups = maxOldBackups;
            BackupFolderPath = backupFolderPath;
            FileList = new();
            Logger = serviceProvider.GetService<Logger>();
        }
        /// <summary>
        /// Creates backup and backup old directory if does not exists.
        /// </summary>
        /// <returns></returns>
        public async Task<Task> Initalize()
        {
            Directory.CreateDirectory(BackupFolderPath);
            Directory.CreateDirectory(BackupFolderPath + "/old");
            return Task.CompletedTask;
        }
        /// <summary>
        /// Register any file that should be included in future backups.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public async Task<Task> RegisterBackupFile(string path)
        {
            if (FileList.Contains(path)) 
            {
                Logger.Log(ModuleName, $"This file is already registered: {path}", LogLevel.Info);
                return Task.CompletedTask; 
            }
            FileList.Add(path);
            Logger.Log(ModuleName, $"New file registered: {path}", LogLevel.Info);
            return Task.CompletedTask;
        }
        /// <summary>
        /// Unregister any file that is currently registered. You need to specify original path that was used to register this file.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public async Task<Task> UnregisterBackupFile(string path)
        {
            if(!FileList.Contains(path)) 
            {
                Logger.Log(ModuleName, $"Given file path does not exist in backup file list: {path}", LogLevel.Info);
                return Task.CompletedTask;
            }
            FileList.Remove(path);
            Logger.Log(ModuleName, $"Backup removed successfully: {path}", LogLevel.Info);
            return Task.CompletedTask;
        }
        /// <summary>
        /// Creates backup in previously specified directory. File name is as follows: "yyyy-MM-dd WindowsFileTime.zip"
        /// </summary>
        /// <returns></returns>
        private async Task<Task> CreateBackup()
        {
            var timer = Stopwatch.StartNew();
            


            var date = DateTime.Now;
            string dateString = date.ToString("yyyy-MM-dd");
            var timeStamp = date.ToFileTime();
            string zipOutput = $"{BackupFolderPath}/{dateString} {timeStamp}.zip";

            var dirFiles = FileList.ToArray();

            await CreateArchive(dirFiles, zipOutput);

            await ManageBackups();
            timer.Stop();
            Logger.Log(ModuleName, $"Backup created {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")} in {timer.ElapsedMilliseconds} ms.", LogLevel.Info);

            return Task.CompletedTask;
        }
        /// <summary>
        /// Checks if number of files in backup folder is greater than the specified threshold, if so, it packs all files into one collective zip file and saves them in the old folder, and deletes them from backup folder.
        /// </summary>
        /// <returns></returns>
        private async Task<Task> ManageBackups()
        {
            var dirFilesCount = Directory.EnumerateFiles(BackupFolderPath, "*", SearchOption.TopDirectoryOnly).Count();

            if (dirFilesCount < MaxBackups) return Task.CompletedTask;

            

            var dirFiles = Directory.GetFiles(BackupFolderPath, "*", SearchOption.TopDirectoryOnly);

            var date = DateTime.Now;
            string dateString = date.ToString("yyyy-MM-dd");
            var timeStamp = date.ToFileTime();

            string zipOutput = $"{BackupFolderPath}/old/{dateString} {timeStamp}.zip";

            //
            var result = await CreateArchive(dirFiles, zipOutput);
            if (result)
                foreach (var file in dirFiles)
                {
                    File.Delete(file);
                }

            await DeleteOldestBackup();
            return Task.CompletedTask;
        }
        /// <summary>
        /// Creates .zip file and puts all files listed in <see cref="string"/> <see cref="Array"/>
        /// </summary>
        /// <param name="dirFiles"></param>
        /// <param name="zipOutput"></param>
        /// <returns></returns>
        private async Task<bool> CreateArchive(string[] dirFiles, string zipOutputPath)
        {
            try
            {
                using (ZipArchive archive = ZipFile.Open(zipOutputPath, ZipArchiveMode.Create))
                {
                    foreach (var file in dirFiles)
                    {
                        archive.CreateEntryFromFile(file, Path.GetFileName(file));
                    }
                }
            }catch (Exception ex)
            {
                return false;
            }
            return true;
        }
        /// <summary>
        /// Checks if number of files in backup folder is greater than the specified threshold, if so, delete the .zip file in old folder.
        /// </summary>
        /// <returns></returns>
        private async Task<bool> DeleteOldestBackup()
        {
            var dirFilesCount = Directory.EnumerateFiles(BackupFolderPath + "/old", "*", SearchOption.TopDirectoryOnly).Count();

            if (dirFilesCount <= MaxOldBackups) return false;

            FileSystemInfo fileInfo = new DirectoryInfo(BackupFolderPath + "/old").GetFileSystemInfos()
            .OrderBy(fi => fi.CreationTime).FirstOrDefault();

            File.Delete(fileInfo.FullName);
            
            return true;
        }
        /// <summary>
        /// Starts backup loop. Default interval is 1200 seconds.
        /// </summary>
        /// <param name="interval"></param>
        /// <returns></returns>
        public async Task<Task> Start(int interval = 1200)
        {
            started = true;
            _ = Task.Run(async () =>
            {
                while (started)
                {
                    await CreateBackup();
                    await Task.Delay(interval * 1000);
                }
            });
            
            return Task.CompletedTask;
        }
        /// <summary>
        /// Stops backup loop.
        /// </summary>
        /// <returns></returns>
        public async Task<Task> Stop()
        {
            started = false;
            return Task.CompletedTask;
        }
    }
}