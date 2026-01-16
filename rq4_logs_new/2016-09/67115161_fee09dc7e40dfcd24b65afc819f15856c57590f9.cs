using UnityEngine;
using UnityEditor;
using UnityEditor.Callbacks;
using System.Collections;

public class AppPostProcessBuild {
	[PostProcessBuild]
	public static void OnPostprocessBuild(BuildTarget target, string path) {
		if (target == BuildTarget.Android) {
//			// Terminalを起動してシェルスクリプトを実行する
//			// 直接シェルスクリプトを実行することもできるが進捗状態が出力されないのでTerminal上で実行させる。
//			var process = new System.Diagnostics.Process ();
//			process.StartInfo.UseShellExecute = false;
//			process.StartInfo.RedirectStandardOutput = true;
//			// AppleScriptを起動（直接Terminalを起動しても何故かシェルスクリプトが実行されない）
//			process.StartInfo.FileName = "osascript";
//			// Terminalを指定してシェルスクリプトを実行する
//			process.StartInfo.Arguments = string.Format ("-e 'tell app \"Terminal\" to do script \"cd {0} && cd ../Script && sh build.sh\"'", path);
//			process.Start ();
//
//			// 終了を待つ
//			string strOutput = process.StandardOutput.ReadToEnd (); 
//			process.WaitForExit ();
//			UnityEngine.Debug.Log (strOutput);
		}
	}
}