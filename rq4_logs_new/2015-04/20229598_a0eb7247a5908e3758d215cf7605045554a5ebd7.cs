//
//  Tealium iOS Build Postprocessor
//  
//  Unity 5.x + Only! This will not work with earlier versions of unity.
//  
//  This is mearly a convenience of add the linker flag "-ObjC" automatically.
//  With previous versions of unity simply adding this to the "Other Linker Flags" build setting of your target will work just fine.


using UnityEngine;
using UnityEditor;
using UnityEditor.Callbacks;

#if UNITY_IOS
using UnityEditor.iOS.Xcode;
#endif

using System.Collections;
using System.IO;

public class TealiumBuildPostprocessor {
    
  [PostProcessBuild]
  public static void OnPostprocessBuild(BuildTarget buildTarget, string pathToBuiltProject) {
      
    #if UNITY_IOS

      string projPath = path + "/Unity-iPhone.xcodeproj/project.pbxproj";
       
      PBXProject proj = new PBXProject ();
      proj.ReadFromString (File.ReadAllText (projPath));

      string target = proj.TargetGuidByName ("Unity-iPhone");

      proj.AddBuildProperty (target, "OTHER_LDFLAGS", "-ObjC");

      File.WriteAllText (projPath, proj.WriteToString ());

    #endif
  }

}