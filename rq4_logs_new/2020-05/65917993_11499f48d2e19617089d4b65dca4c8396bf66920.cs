using System;
using System.IO;
using Ctlg.Core;
using Ctlg.Core.Interfaces;
using Newtonsoft.Json;

namespace Ctlg.Service.Services
{
    public class JsonConfigService : IConfigService
    {
        public JsonConfigService(IFilesystemService filesystemService)
        {
            FilesystemService = filesystemService;
        }

        public Config LoadConfig()
        {
            var dir = FilesystemService.GetCurrentDirectory();
            var configPath = FilesystemService.CombinePath(dir, "config.json");

            if (FilesystemService.FileExists(configPath))
            {
                using (var reader = new StreamReader(FilesystemService.OpenFileForRead(configPath)))
                {
                    var json = JsonConvert.DeserializeObject<JsonConfig>(reader.ReadToEnd());

                    return new Config
                    {
                        Path = dir,
                        HashAlgorithmName = json.HashAlgorithm
                    };
                }
            }

            return new Config
            {
                Path = dir,
                HashAlgorithmName = "SHA-256"
            };
        }

        private IFilesystemService FilesystemService { get; }
    }
}