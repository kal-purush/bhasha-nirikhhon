/* 
*   NatCorder
*   Copyright (c) 2017 Yusuf Olokoba
*/

namespace NatCorderU.Utilities {

    using UnityEditor;
    using UnityEditor.Callbacks;
    using System;
	using System.Collections.Generic;
    using System.IO;

    #if UNITY_IOS
    using UnityEditor.iOS.Xcode;
    #endif

    public static class NatCorderEditor {

        private const string
		LibraryUsageKey = @"NSPhotoLibraryAddUsageDescription",
		LibraryUsageDescription = @"Allow this app to save videos to your photo library", // Change this as necessary
        API = "NATCORDER",
        CBridge = "INATCORDER_C",
		VersionNumber = "NATCORDER_10";

        [InitializeOnLoadMethod]
		static void SetGlobalDefines () {
			// Define C bridge dependency // Needed to compile AOT (IL2CPP)
			BuildTargetGroup[] bridgePlatforms = {BuildTargetGroup.iOS, BuildTargetGroup.WebGL, BuildTargetGroup.Standalone};
			// Iterate and set
			foreach (var target in new [] {BuildTargetGroup.Android, BuildTargetGroup.iOS, BuildTargetGroup.Standalone, BuildTargetGroup.WebGL, BuildTargetGroup.WSA}) {
				string current = PlayerSettings.GetScriptingDefineSymbolsForGroup(target);
                if (!current.Contains(API)) current += (current.Equals(string.Empty) ? "" : ";") + API;
				if (!current.Contains(VersionNumber)) current += ";" + VersionNumber;
				if (Array.Exists(bridgePlatforms, t => t == target) && !current.Contains(CBridge)) current += ";" + CBridge;
				else if (!Array.Exists(bridgePlatforms, t => t == target) && current.Contains(CBridge)) current = current.Replace(";" + CBridge, string.Empty);
				PlayerSettings.SetScriptingDefineSymbolsForGroup(target, current);
			}
		}


        #if UNITY_IOS

		[PostProcessBuild]
		static void LinkFrameworks (BuildTarget buildTarget, string path) {
			if (buildTarget != BuildTarget.iOS) return;
			string projPath = path + "/Unity-iPhone.xcodeproj/project.pbxproj";
			PBXProject proj = new PBXProject();
			proj.ReadFromString(File.ReadAllText(projPath));
			string target = proj.TargetGuidByName("Unity-iPhone");
			foreach (var framework in new [] {"Accelerate.framework", "AssetsLibrary.framework", "CoreImage.framework", "Photos.framework"}) proj.AddFrameworkToProject(target, framework, true);
			File.WriteAllText(projPath, proj.WriteToString());
		}

		[PostProcessBuild]
		static void SetPermissions (BuildTarget buildTarget, string path) {
			if (buildTarget != BuildTarget.iOS) return;
			string plistPath = path + "/Info.plist";
			PlistDocument plist = new PlistDocument();
			plist.ReadFromString(File.ReadAllText(plistPath));
			PlistElementDict rootDictionary = plist.root;
			rootDictionary.SetString(LibraryUsageKey, LibraryUsageDescription);
			File.WriteAllText(plistPath, plist.WriteToString());
		}
		#endif
    }
}
#pragma warning restore 0162, 0429