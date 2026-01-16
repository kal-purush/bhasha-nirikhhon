using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LaserBeam : MonoBehaviour
{
    public float durationInSeconds;
    public float distance;
    public float velocity;
    Vector3 originPos;
    Vector3 verticalPos = new Vector3(1,0.8f,2.3f);
    float time;
    int count = 0;
    // Start is called before the first frame update
    void Start()
    {
        originPos = gameObject.transform.position;
    }

    // Update is called once per frame
    void Update()
    {
        if (gameObject.transform.position.z >= -1)
        {
            gameObject.transform.position -= new Vector3(0, 0, velocity * Time.deltaTime);
            
        }
        else { 
            count++;
            if (count % 2 == 0)
            {
                gameObject.transform.rotation = Quaternion.Euler(0, 0, 0);
                gameObject.transform.position = originPos;
            }
            else
            {
                gameObject.transform.rotation = Quaternion.Euler(0, 0, 90);
                gameObject.transform.position = verticalPos;
            }
        }
        
    }
}