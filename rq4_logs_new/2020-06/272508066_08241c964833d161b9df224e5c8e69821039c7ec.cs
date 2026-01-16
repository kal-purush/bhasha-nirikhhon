using System.Collections;
using System.Collections.Generic;
using Assets.Scripts;
using UnityEngine;

public class BuoyancyCraft : MonoBehaviour
{
    public Transform ObjectToSpawn;
    public Vector3 ErrorAngle;
    public float SpawningCooldown = 20;

    private FrameLocker _fl = new FrameLocker();


    // Start is called before the first frame update
    void Start()
    { 

        _fl.LockSeconds = SpawningCooldown;
    }

    // Update is called once per frame
    void Update()
    {
        if (_fl.CheckTime())
        {
            Transform obj = Instantiate(ObjectToSpawn, transform);
            obj.rotation = Quaternion.Euler(obj.rotation.eulerAngles.x + ErrorAngle.x, obj.rotation.eulerAngles.y + ErrorAngle.y, obj.rotation.eulerAngles.z + ErrorAngle.z);

            _fl.StartCountdown();
        }

        _fl.Countdown();
    }
}