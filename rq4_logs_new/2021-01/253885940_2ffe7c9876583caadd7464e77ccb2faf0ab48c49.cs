using AutoVersionsDB.Helpers;
using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace AutoVersionsDB.Core.DBVersions.ScriptFiles
{
    public class RuntimeScriptFile
    {
        public const string TargetNoneScriptFileName = "#None";
        public const string TargetLastScriptFileName = "#Last";


        public static bool TryParseNextRuntimeScriptFileInstance(ScriptFileTypeBase scriptFileType, string folderPath, string scriptName, RuntimeScriptFile prevRuntimeScriptFile, out RuntimeScriptFile newRuntimeScriptFile)
        {
            int nextOrderNum = 1;

            if (prevRuntimeScriptFile != null)
            {
                nextOrderNum = prevRuntimeScriptFile.OrderNum + 1;
            }

            newRuntimeScriptFile = new RuntimeScriptFile(scriptFileType, folderPath, scriptName, nextOrderNum);

            return newRuntimeScriptFile.IsValidFileName;
        }
        //public static RuntimeScriptFile CreateRuntimeScriptFileInstanceByFilename<TScriptFileType>(string folderPath, string fileFullPath)
        //    where TScriptFileType : ScriptFileTypeBase, new()
        //{
        //    return new RuntimeScriptFile(new TScriptFileType(),folderPath, fileFullPath);
        //}


        public ScriptFileTypeBase ScriptFileType { get; }


        public int OrderNum { get; set; }
        public string ScriptName { get; protected set; }

        public string SortKey => $"{OrderNum:0000}{ScriptName}";

        public string Filename => $"{ScriptFileType.Prefix}_{OrderNum:0000}_{ScriptName}.sql";


        public virtual string FolderPath { get; protected set; }

        public bool IsValidFileName => Regex.IsMatch(Filename, ScriptFileType.RegexFilenamePattern);

        public string FileFullPath => Path.Combine(FolderPath, Filename);

        public string ComputedHash { get; set; }
        public DateTime ComputedHashDateTime { get; set; }
        public HashDiffType HashDiffType { get; set; }

        public DateTime ExecutedDateTime { get; set; }



        public RuntimeScriptFile(ScriptFileTypeBase scriptFileType, string folderPath, string scriptName, int orderNum)
        {
            ScriptFileType = scriptFileType;
            FolderPath = folderPath;
            ScriptName = scriptName;
            OrderNum = orderNum;
        }

        public RuntimeScriptFile(ScriptFileTypeBase scriptFileType, string folderPath, string fileFullPath)
        {
            scriptFileType.ThrowIfNull(nameof(scriptFileType));
            fileFullPath.ThrowIfNull(nameof(fileFullPath));
            folderPath.ThrowIfNull(nameof(folderPath));

            ScriptFileType = scriptFileType;
            FolderPath = folderPath;


            FileInfo fiFile = new FileInfo(fileFullPath);

            string shouldBeFileFullPath = Path.Combine(FolderPath, fiFile.Name);

            if (shouldBeFileFullPath.Trim().ToUpperInvariant() != fileFullPath.Trim().ToUpperInvariant())
            {
                throw new ArgumentException($"The argument path: '{fileFullPath}' is different from '{shouldBeFileFullPath}'");
            }

            string filename = fiFile.Name;
            string filenameWithoutExtension = fiFile.Name.Replace(fiFile.Extension, "");


            bool isFilenameValid = Regex.IsMatch(filename, ScriptFileType.RegexFilenamePattern);

            if (!isFilenameValid)
            {
                string errorMessage =
                    CoreTextResources
                    .InvalidFilenameErrorMessage
                    .Replace("[Filename]", filename)
                    .Replace("[FileTypeCode]", ScriptFileType.FileTypeCode)
                    .Replace("[FilenamePattern]", ScriptFileType.FilenamePattern);

                throw new Exception(errorMessage);
            }

            string[] arrFilenameParts = Regex.Split(filenameWithoutExtension, "_");

            ScriptName = string.Join("_", arrFilenameParts.Skip(2));

            string fileOrderNumStr = arrFilenameParts[1];

            if (!int.TryParse(fileOrderNumStr, out int tempOrderNum_FromFilename))
            {
                string errorMessage =
                    CoreTextResources
                    .InvalidOrderNumForIncScriptException
                    .Replace("[Filename]", filename);

                throw new Exception(errorMessage);
            }

            OrderNum = tempOrderNum_FromFilename;
        }

    }


}