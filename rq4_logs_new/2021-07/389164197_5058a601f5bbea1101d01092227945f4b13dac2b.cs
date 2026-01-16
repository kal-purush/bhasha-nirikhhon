using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class KeyScript : MonoBehaviour
{
    [SerializeField] private GameObject player;
    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        Debug.Log("Hello");
        checkPlayerTouch();   
    }

    private void checkPlayerTouch()
    {
        if (Vector3.Distance(gameObject.transform.position, player.transform.position) <= 0.6f)
        {
            Debug.Log("Touch Key");
            EventBroadcaster.Instance.PostEvent(EventNames.ON_KEY_GET);
            Destroy(this.gameObject);
        }
    }
}