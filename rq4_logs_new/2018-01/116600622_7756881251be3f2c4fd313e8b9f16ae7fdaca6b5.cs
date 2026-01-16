using UnityEditor;

class Build
{
    static string[] scenes = {"Assets/Scenes/Title.unity",
                            "Assets/scenes/forest.unity",
                            "Assets/scenes/join.unity",
                            "Assets/scenes/host.unity",
                            "Assets/scenes/barrels.unity"};

    static void BuildWebGl()
    {
        string pathToDeploy = "build/webgl/";

        BuildPipeline.BuildPlayer(scenes, pathToDeploy, BuildTarget.WebGL, BuildOptions.None);
    }

    static void BuildWindows()
    {
        string pathToDeploy = "build/windows/";

        BuildPipeline.BuildPlayer(scenes, pathToDeploy, BuildTarget.StandaloneWindows, BuildOptions.None);
    }

    static void BuildLinux()
    {
        string pathToDeploy = "build/linux/";

        BuildPipeline.BuildPlayer(scenes, pathToDeploy, BuildTarget.StandaloneLinuxUniversal, BuildOptions.None);
    }
}