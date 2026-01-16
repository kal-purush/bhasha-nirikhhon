using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Photon.Pun;
using UnityEngine.UI;

public class Logic : MonoBehaviour
{
    public Button b;
    public Text t;
    // Start is called before the first frame update
    void Start()
    {

    
            PhotonNetwork.AutomaticallySyncScene = true;
        
        /*if (LobbyManager.userType)
        {*/
            /*b.interactable = false;
            b.image.color = new Color(b.image.color.r, b.image.color.g, b.image.color.b, 0);
            t.text = "";*/
       // }
        
    }

    
}