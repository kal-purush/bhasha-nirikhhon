using UnityEditor;
using UnityEditor.Callbacks;
using UnityEditor.iOS.Xcode;
 
public class XCodePostProcess
{
    [PostProcessBuild]
    public static void OnPostprocessBuild(BuildTarget target, string path)
    {
        if (target == BuildTarget.iOS)
        {
            string projPath = PBXProject.GetPBXProjectPath(path);
            PBXProject proj = new PBXProject();
            proj.ReadFromFile(projPath);
 
            string targetGuid = proj.GetUnityMainTargetGuid();
 
            foreach (var framework in new[] { targetGuid, proj.GetUnityFrameworkTargetGuid() })
            {
                proj.SetBuildProperty(framework, "ENABLE_BITCODE", "NO");
            }
 
            proj.WriteToFile(projPath);
        }
    }
}