using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerWeaponManager : MonoBehaviour
{

    public static GameObject pistol;

    public static GameObject assaultRifle;

    public static GameObject shotgun;

    public static GameObject rocketLauncher;

    private static GameObject currentWeapon;

    // Start is called before the first frame update
    void Start()
    {
        AssignWeapons();
        SwitchToPistol();
    }

    void AssignWeapons()
    {
        // find in the children of the player:
        GameObject player = GameObject.Find("Main Camera");
        pistol = player.transform.Find("Pistol").gameObject;
        // assaultRifle = player.transform.Find("AssaultRifle").gameObject;
        // shotgun = player.transform.Find("Shotgun").gameObject;
        rocketLauncher = player.transform.Find("RocketLauncher").gameObject;

        Debug.Log("Pistol: " + pistol);
        // Debug.Log("Assault Rifle: " + assaultRifle);
        // Debug.Log("Shotgun: " + shotgun);
        Debug.Log("Rocket Launcher: " + rocketLauncher);
    }

    public static void SwitchToPistol()
    {
        pistol.SetActive(true);
        // assaultRifle.SetActive(false);
        // shotgun.SetActive(false);
        rocketLauncher.SetActive(false);
    }

    public static void SwitchToAssaultRifle()
    {
        assaultRifle.SetActive(true);
        currentWeapon = assaultRifle;
        currentWeapon.SetActive(true);
        pistol.SetActive(false);
        shotgun.SetActive(false);
        rocketLauncher.SetActive(false);
    }

    public static void SwitchToShotgun()
    {
        shotgun.SetActive(true);
        pistol.SetActive(false);
        assaultRifle.SetActive(false);
        rocketLauncher.SetActive(false);
    }

    public static void SwitchToRocketLauncher()
    {
        rocketLauncher.SetActive(true);
        pistol.SetActive(false);
        // assaultRifle.SetActive(false);
        // shotgun.SetActive(false);
    }
}