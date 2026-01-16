using UnityEngine;
using System.Collections;

public class ScreenShotBridge 
{
	static string path = "com.plugin.screenshot.ScreenShotPlugin";
	public delegate void ScreenShotDelegate(bool screenShotStatus);
	public static IEnumerator SaveScreenShot(string fileName,string albumName,bool isScreenShotWithDateTime,ScreenShotDelegate callBack)
	{
		yield return new WaitForEndOfFrame ();
		Application.CaptureScreenshot (fileName+".png");
		if (Application.platform == RuntimePlatform.Android) {
						string origin = System.IO.Path.Combine (Application.persistentDataPath, fileName + ".png");
						string destination = "/sdcard/" + albumName + "/";
						if (!System.IO.Directory.Exists ("/sdcard/" + albumName)) {
								System.IO.Directory.CreateDirectory (destination);
						}
						if (System.IO.File.Exists (origin)) {
								string finalFileName = "";
								if (isScreenShotWithDateTime)
										finalFileName = destination + "" + fileName + System.DateTime.Now.ToString ("yyyyMMddHHmmssfff") + ".png";
								else {
										int totalScreenShots = PlayerPrefs.GetInt ("TotalScreenShots", 0);
										finalFileName = destination + "" + fileName + "_" + totalScreenShots + ".png";
										totalScreenShots++;
										PlayerPrefs.SetInt ("TotalScreenShots", totalScreenShots);
								}
								System.IO.File.Move (origin, finalFileName);
								AndroidJavaClass playerClass = new AndroidJavaClass ("com.unity3d.player.UnityPlayer");
								AndroidJavaObject activity = playerClass.GetStatic<AndroidJavaObject> ("currentActivity");		
								AndroidJavaClass pluginClass = new AndroidJavaClass (path);
								pluginClass.CallStatic ("RefreshGallery", new object[2] {activity, finalFileName});
						}
				}
		callBack(true);
	}
}