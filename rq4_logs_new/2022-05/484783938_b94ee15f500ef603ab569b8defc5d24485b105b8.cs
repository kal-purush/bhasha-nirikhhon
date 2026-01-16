using System.Diagnostics;
using UnityEngine;

public class LocalPlaytestBootstrap : MonoBehaviour
{
	private const string PlayfabRelativePath = @"..\KAG.Playfab";
	
	private Process _playfabLocalProcess;

	private void Awake()
	{
		if (!Application.isEditor)
			return;

		_playfabLocalProcess = new Process
		{
			StartInfo =
			{
				WorkingDirectory = PlayfabRelativePath,
				FileName = "LocalMultiplayerAgent.exe",
				WindowStyle = ProcessWindowStyle.Hidden,
				CreateNoWindow = true,
				UseShellExecute = true
			}
		};

		_playfabLocalProcess.Start();
	}

	private void OnApplicationQuit()
	{
		if (!Application.isEditor)
			return;
		
		if (_playfabLocalProcess != null && !_playfabLocalProcess.HasExited)
			_playfabLocalProcess.Kill();
		
		var shutdownPlayfabLocalProcess = new Process
		{
			StartInfo =
			{
				WorkingDirectory = PlayfabRelativePath,
				FileName = "powershell.exe",
				Arguments = ".\\ShutdownLocalMultiplayerAgent.ps1",
				WindowStyle = ProcessWindowStyle.Hidden,
				CreateNoWindow = true,
				UseShellExecute = true
			}
		};

		shutdownPlayfabLocalProcess.Start();
	}
}