using UnityEngine;
using UnityEngine.Networking;
using UnityEngine.Rendering;

public class ModeManager : MonoBehaviour {

    int port = 34920;


    bool IsHeadless()
    {
        return SystemInfo.graphicsDeviceType == GraphicsDeviceType.Null;
    }


    void Start()
    {
        if (IsHeadless())
        {
            NetworkManager.singleton.networkAddress = "0.0.0.0";
            NetworkManager.singleton.networkPort = port;
            Debug.Log("Creating match [" + NetworkManager.singleton.networkAddress + ":" + NetworkManager.singleton.networkPort + "]");
            mode = "server";
            NetworkManager.singleton.StartServer();
        }
        else if (Application.isEditor)
        {
            Debug.Log("Running Local Server");
            mode = "local";
            NetworkManager.singleton.StartHost();
        }
        else
        {
            NetworkManager.singleton.networkAddress = "core2.bitvox.me";
            NetworkManager.singleton.networkPort = port;
            Debug.Log("Joining server [" + NetworkManager.singleton.networkAddress + ":" + NetworkManager.singleton.networkPort + "]");
            mode = "client";
            NetworkManager.singleton.StartClient();
        }
    }

    string mode;
	void OnGUI()
	{
			GUI.Label(new Rect(10, 50, 460,20), "Mode:" + mode);
	}

    void Update()
    {

    }
}
