using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CentralPivot : MonoBehaviour
{
    private Spawner spawner;
    private float rotationControl;

    private Player player;
    private bool hardcore=false;
    private int tryHard=0;
    
    void Awake(){
        spawner=GameObject.FindWithTag("spawner").GetComponent<Spawner>();
        player=GameObject.FindWithTag("player").GetComponent<Player>();
    }

    void Start(){
        rotationControl=transform.eulerAngles.z+45;//20 extra antes de espawnar
        Debug.Log(rotationControl);
    }

    void Update()
    {
        if(!hardcore){
            if(transform.eulerAngles.z-rotationControl>=6){
                spawner.SpawnGate();
                rotationControl=transform.eulerAngles.z;
            }
            if(transform.eulerAngles.z>90)player.difficulty=2;
            if(transform.eulerAngles.z>150)player.difficulty=3;
            if(transform.eulerAngles.z>270)player.difficulty=4;
            if(transform.eulerAngles.z>358)hardcore=true;
        }
        else{
            int hardcoreness=1;
            if(tryHard==0){
                if(transform.eulerAngles.z>90)hardcoreness=2;
                if(transform.eulerAngles.z>150)hardcoreness=3;
                if(transform.eulerAngles.z>270)hardcoreness=4;
                if(transform.eulerAngles.z>350)tryHard++;
                if(transform.eulerAngles.z-rotationControl>=6-hardcoreness){
                    spawner.SpawnGate();
                    rotationControl=transform.eulerAngles.z;
                }
            }
            else if(tryHard==1){
                if(transform.eulerAngles.z>90)hardcoreness=3;
                if(transform.eulerAngles.z>150)hardcoreness=4;
                if(transform.eulerAngles.z>270)hardcoreness=5;
                if(transform.eulerAngles.z>350)tryHard++;
                if(transform.eulerAngles.z-rotationControl>=6-hardcoreness){
                    spawner.SpawnGate();
                    rotationControl=transform.eulerAngles.z;
                }
            }
            else if(tryHard==2){
                if(transform.eulerAngles.z>90)hardcoreness=5;
                if(transform.eulerAngles.z>150)hardcoreness=5;
                if(transform.eulerAngles.z>270)hardcoreness=5;
                if(transform.eulerAngles.z>350)tryHard++;
                if(transform.eulerAngles.z-rotationControl>=6-hardcoreness){
                    spawner.SpawnGate();
                    rotationControl=transform.eulerAngles.z;
                }
            }
            else if(tryHard==3){
                spawner.SpawnGate();
                spawner.SpawnGate();
                spawner.SpawnGate();
            }
        }
    }
}