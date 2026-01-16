using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        EventBroadcaster.Instance.PostEvent(EventNames.ON_KEY_LOST);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}