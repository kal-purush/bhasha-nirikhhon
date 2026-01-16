using UnityEditor;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Collections.Generic;

class BuildProcess {

    [MenuItem("UnityBuilds/WebGL")]
    static void BuildWebGL() {
        string path = "Builds/WebGL/"+GetBuildTime();
        string[] sceneFiles = GetSceneFiles();

        BuildPipeline.BuildPlayer(sceneFiles,path, BuildTarget.WebGL, BuildOptions.None);

        CopyFile(path, "Assets/Editor/WebGLDeployment~", "package.json");
        CopyFile(path, "Assets/Editor/WebGLDeployment~", "server.js");
        CopyFile(path, "Assets/Editor/WebGLDeployment~", "run.sh");
    }

    static void CopyFile(string destPath, string localPath, string file) {
        FileUtil.CopyFileOrDirectory(localPath + "/" + file, destPath + "/" + file);
    }

    static string GetBuildTime() {
        System.DateTime saveUtcNow = System.DateTime.UtcNow;
        return saveUtcNow.ToString("yyyy_M_d-HH_mm_ss");
    }

    static string[] GetSceneFiles() {
        List<string> scenes = new List<string>();

        foreach(EditorBuildSettingsScene scene in EditorBuildSettings.scenes) {
            if (!scene.enabled) {
                continue;
            }

            scenes.Add(scene.path);
        }

        return scenes.ToArray();
    }
  }