using System;
using System.Collections;
using System.Net;
using TMPro;
using UnityEngine;
using Mirror;
using UnityEngine.SceneManagement;

public class MenuScript : MonoBehaviour
{
    private NetworkManager netManager;
    private PlayerData playerData;
    public int defaultPort = 7777;

    [Header("Windows")]
    public CanvasGroup MainWindow;
    public CanvasGroup PlayWindow;
    
    [Header("Input Fields for Main Menu")]
    public TMP_Text usernameInput;
    
    [Header("Input Fields for Play Menu")]
    public TMP_Text addressConnect;
    public TMP_Text portConnect;

    public TMP_Text portHost;

    [Header("Scene Settings")] 
    public int StageID = 1;

    [SerializeField]private Scene _stageName;
    
    private void Start()
    {
        portConnect.text = defaultPort.ToString();
        addressConnect.text = "127.0.0.1";
        portHost.text = defaultPort.ToString();
        netManager = NetworkManager.singleton;
        MainWindow.alpha = 1;
        MainWindow.blocksRaycasts = true;
        PlayWindow.alpha = 0;
        PlayWindow.blocksRaycasts = false;
    }
    


    public void OnStart()
    {
        //playerData = new PlayerData(usernameInput.GetComponent<TMP_InputField>().text);
        Debug.Log(usernameInput.GetComponent<TMP_Text>().text);

        if (!SceneManager.GetSceneByName("SampleScene").isLoaded)
        {
            SceneManager.LoadScene("SampleScene", LoadSceneMode.Additive);
        }

        StartCoroutine(CheckSceneLoaded("SampleScene"));
        OnWindowSwap();
    }

    private IEnumerator CheckSceneLoaded(string sceneName)
    {
        Scene scene = SceneManager.GetSceneByName(sceneName);
        while (!scene.isLoaded)
        {
            yield return null; // Wait for the next frame
            scene = SceneManager.GetSceneByName(sceneName);
        }

        _stageName = scene;
        Debug.Log($"Scene '{_stageName.name}' is successfully loaded.");
    }

    public void OnExit()
    {
        Application.Quit();
    }

    public void OnReturn()
    {
        OnWindowSwap();
    }

    private void OnWindowSwap()
    {
        if (MainWindow.alpha == 1 && PlayWindow.alpha == 0)
        {
            MainWindow.alpha = 0;
            MainWindow.blocksRaycasts = false;
            PlayWindow.alpha = 1;
            PlayWindow.blocksRaycasts = true;
        }
        else if (MainWindow.alpha == 0 && PlayWindow.alpha == 1)
        {
            MainWindow.alpha = 1;
            MainWindow.blocksRaycasts = true;
            PlayWindow.alpha = 0;
            PlayWindow.blocksRaycasts = false;
        }
        else
        {
            MainWindow.alpha = 1;
            MainWindow.blocksRaycasts = true;
            PlayWindow.alpha = 0;
            PlayWindow.blocksRaycasts = false;
        }
    }

    private void OnSceneChange()
    {
        MainWindow.alpha = 0;
        MainWindow.blocksRaycasts = false;
        PlayWindow.alpha = 0;
        PlayWindow.blocksRaycasts = false; 
    }

    public void OnHost()
    {
        ushort port;
        if (ushort.TryParse(portHost.text, out port))
        {
            var transport = Transport.active as TelepathyTransport;
            if (transport != null)
            {
                transport.port = port;
                Debug.Log($"Host port set to {port}");
            }
            else
            {
                Debug.LogError("Current port is not valid");
                return;
            }
            netManager.transport = transport;
        }
        SceneManager.SetActiveScene(_stageName);
        if (SceneManager.GetActiveScene().name != "MainMenu")
        {
            netManager.StartHost();
            OnSceneChange();
            Debug.Log("Hosting started in the loaded scene.");
        }
        else
        {
            Debug.LogError("No valid scene is loaded for hosting.");
        }
    }

    public void OnConnect()
    {
        ushort port;
        IPAddress address;
        var transport = Transport.active as TelepathyTransport;
        if (ushort.TryParse(portConnect.text, out port))
        {
            if (transport != null)
            {
                transport.port = port;
            }
        }
        else
        {
            Debug.LogError("Current port is not valid");
            return;
        }
        if (IPAddress.TryParse(addressConnect.text, out address))
        {
            netManager.networkAddress = address.ToString();
            netManager.StartClient();
        }
        else
        {
            Debug.LogError("Current address is not valid");
            return;
        }
    }
}