using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class GunController : MonoBehaviour
{
    static GunController _instance;

    static GunController Instance
    {
        get
        {
            if (_instance == null)
            {
                _instance = FindObjectOfType<GunController>();
                if (_instance == null)
                {
                    throw new Exception("Cannot find GunController instance.");
                }
            }
            return _instance;
        }
    }

    List<GunBehavior> guns;
    GunBehavior currentGun;
    int gunIndex;

    private void Start() {
        guns = new List<GunBehavior>(FindObjectsOfType<GunBehavior>());
        if(guns.Count == 0){
           throw new Exception("Theres no gun in scene!");
        }
        currentGun = guns[0];
        gunIndex = 0;
        currentGun.Planet.Select(true);

    }


    public static void MoveToLeft()
    {
        Instance.currentGun.MoveToLeft();
    }

    public static void MoveToRight()
    {
        Instance.currentGun.MoveToRight();
    }

    public static void Stop()
    {
        Instance.currentGun.Stop();
    }

    public static void Shoot()
    {
        Instance.currentGun.Shoot();
    }

    public static void SwitchPlanet(float dir)
    {
        Instance.currentGun.Stop();
        Instance.currentGun.Planet.Select(false);


        if(dir > 0){
            Instance.gunIndex++;
            if((Instance.gunIndex) == Instance.guns.Count){
                Instance.gunIndex = 0;
            }
        }
        else{
            Instance.gunIndex--;
            if((Instance.gunIndex) == -1){
                Instance.gunIndex = Instance.guns.Count-1;
            }
        }
        Instance.currentGun = Instance.guns[Instance.gunIndex];
        Instance.currentGun.Planet.Select(true);

    }
}