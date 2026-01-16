using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Bomb : MonoBehaviour
{
    [SerializeField]
    [Tooltip ("A timer in seconds how long till the bomb fizzles out onces its been activated")]
    private float timer;
    public bool active {get; set;}

    private float counter;
    // Start is called before the first frame update
    void Start()
    {
        counter = timer;
        active = false;
    }

    // Update is called once per frame
    void Update()
    {
        if(active){
          //  Debug.Log("active");
            counter -= Time.deltaTime;

        }

        if(counter <= 0){
            Destroy(this.gameObject);
        }
    }
}