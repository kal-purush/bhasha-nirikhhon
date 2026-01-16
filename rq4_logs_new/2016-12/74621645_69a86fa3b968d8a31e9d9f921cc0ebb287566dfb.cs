using UnityEngine;
using System.Collections;

public class BasketDetectorPractice : MonoBehaviour
{

    public GameObject Player;

    public GameObject[] Zones;
    public ScoreManagerPractice scoreManager; 
    Camera camera;
    
    string varabar = "hello dubdubdubd"; 

    //public Transform Teleport;
    //public Transform[] teleportZone;
    //var teleportZone : Transform[]


    void OnTriggerExit(Collider other)
    {
        if (other.attachedRigidbody.velocity.y < 0f)
        {
            //GameManager.score++;
            GameManagerPractice.score++;

            int zoneIndex = Random.Range(0, Zones.Length);
            Player.transform.position = Zones[zoneIndex].transform.position;
            //Player.transform.rotation = Zones[zoneIndex].transform.rotation;

            //camera = GetComponentInParent<Camera>();
            //GameObject hands = GameObject.Find("Hands");
            //hands.transform.position = camera.transform.rotation.ToEulerAngles();
            //Player.transform.position = Teleport.transform.position;

            Debug.Log("detector");

            //scoreManager = new ScoreManagerPractice();
            //scoreManager.Start(varabar);
            //scoreManager.MakeCall("Hoop da da da");
            
            

        }
    }
}