using System.IO;
using UnityEditor.Build;
using UnityEditor.Build.Reporting;
using UnityEngine;

namespace JSContainer.Editor
{
    /// <summary>
    ///     Clear Script require V8 native library to be present in the target folder at proper location
    ///     This script copy ClearScriptV8.win-x64.dll to the Data/MonoBleedingEdge folder
    ///     If you make build for macOS or android you need different version of V8.dll
    ///     e.g. for android
    ///     <see>
    ///         this PulRequest
    ///         <cref>https://github.com/microsoft/ClearScript/pull/290</cref>
    ///     </see>
    /// </summary>
    public class BuildPostProcess : IPostprocessBuildWithReport
    {
        private const string DLL_ORIGIN_PATH =
            "Assets/3DParty/ClearScript/Plugins/runtimes/win-x64/native/ClearScriptV8.win-x64.dll";

        public int callbackOrder => 0;

        public void OnPostprocessBuild(BuildReport report)
        {
            CopyDll(report.summary.outputPath);
        }

        private static void CopyDll(string outputExePath)
        {
            var dataFolderName = Path.GetDirectoryName(outputExePath);
            var fileName = Path.GetFileName(DLL_ORIGIN_PATH);
            var targetFilePath = Path.Combine(dataFolderName!, fileName);
            if (!File.Exists(targetFilePath))
            {
                Debug.Log($"Copy native dll from {DLL_ORIGIN_PATH} to dataFolder {targetFilePath}");
                File.Copy(DLL_ORIGIN_PATH, targetFilePath);
            }
        }
    }
}