using Unity.Netcode;
using Unity.Netcode.Transports.UNET;
using UnityEngine;
using UnityEngine.SceneManagement;

public class IPManager : MonoBehaviour
{
    public NetworkManager nMan;
    public static IPManager Singleton;

    //public PlayerObject GetPlayerObject(ulong id) => networkManager.ConnectedClinets[id].PlayerObject.GetComponent<PlayerObject>();

    public ulong LocalId => nMan.LocalClient.ClientId;

    public UNetTransport Transport => (UNetTransport) nMan.NetworkConfig.NetworkTransport;
    public void SetIP(string ip) => Transport.ConnectAddress = ip;

    private void Start()
    {
        nMan = NetworkManager.Singleton;
        nMan.OnServerStarted += LoadScene;
    }

    private void LoadScene()
    {
        nMan.SceneManager.LoadScene("Game", LoadSceneMode.Single);
    }

    private void Awake()
    {
        DontDestroyOnLoad(this);
        if (Singleton != null && Singleton != this)
        {
            Destroy(gameObject);
        }
        else
        {
            Singleton = this;
        }
        DontDestroyOnLoad(this);
    }
}