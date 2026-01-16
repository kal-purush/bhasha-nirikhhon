using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Sound_Manager : MonoBehaviour
{
    #region Variables

    public AudioClip plantInGound1;

    #endregion

    #region Singleton

    // create variable for storing singleton that any script can access
    private static Sound_Manager instance;

    // create GameManager
    private Sound_Manager()
    {

    }

    // Property for Singleton
    public static Sound_Manager Instance
    {
        get
        {
            // If the singleton does not exist
            if (instance == null)
            {
                // create and return it
                instance = new Sound_Manager();
            }

            // otherwise, just return it
            return instance;
        }
    }

    #endregion
}