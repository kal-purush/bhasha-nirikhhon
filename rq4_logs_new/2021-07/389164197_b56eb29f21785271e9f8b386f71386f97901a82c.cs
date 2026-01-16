using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerRespawn : MonoBehaviour
{
    [SerializeField] private Transform[] spawnLocations;

    // Start is called before the first frame update
    void Start()
    {
        EventBroadcaster.Instance.AddObserver(EventNames.ON_PLAYER_RESPAWN, this.Respawn);
    }

    private void OnDestroy()
    {
        EventBroadcaster.Instance.RemoveObserver(EventNames.ON_PLAYER_RESPAWN);

    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private void Respawn()
    {
        Vector3 myLocation = this.transform.localPosition;
        Vector3 newLocation = this.spawnLocations[Random.Range(0, 5)].position;
        myLocation.x = newLocation.x;
        myLocation.y = newLocation.y;
        myLocation.z = newLocation.z;
        this.transform.position = myLocation;
    }

}