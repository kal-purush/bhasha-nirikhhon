using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LoaderGodScript : MonoBehaviour
{
    [SerializeField]
    public GameObject[] otherPlayers;

    public int numberOtherPlayers;

    // Start is called before the first frame update
    void Start()
    {
        foreach (GameObject item in otherPlayers)
        {
            item.SetActive(false);
        }

        numberOtherPlayers = otherPlayers.Length < numberOtherPlayers ? otherPlayers.Length : numberOtherPlayers;
        for (int i = 0; i < numberOtherPlayers; i++)
        {
            otherPlayers[i].SetActive(true);
        }
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}