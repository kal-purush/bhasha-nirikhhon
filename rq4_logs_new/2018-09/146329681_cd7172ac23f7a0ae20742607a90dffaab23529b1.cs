using UnityEditor;

public class Build {
    public static void PerformBuild() {
        BuildPipeline.BuildPlayer(new[] {"Assets/Scenes/SampleScene.unity"}, "Build/GitDemo.apk",
            BuildTarget.Android, BuildOptions.Development);
    }
}