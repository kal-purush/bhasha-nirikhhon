using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;

public class WeaponHolder : MonoBehaviour
{
    public Animator handsController;
    public GameObject holdPoint;
    public UnityEvent onSwapFinish = new UnityEvent();
    private Dictionary<GameObject, GameObject> existingWeapons = new Dictionary<GameObject, GameObject>();

    private GameObject currentWeapon;
    private GameObject newWeapon;

    public void StartSwap(GameObject weapon, bool firstTime = false)
    {
        if (firstTime) 
            handsController.Play("FirstWeaponSwap");
        else handsController.Play("SwapWeapons");
        newWeapon = weapon;
    }

    //called from the swap animation event
    private void Swap()
    {
        if (existingWeapons.TryGetValue(newWeapon, out GameObject instance))
        {
            instance.SetActive(true);
            if (currentWeapon != null)
            {
                currentWeapon.SetActive(false);
            }
            currentWeapon = instance;

        }
        else
        {
            GameObject newInstance = Instantiate(newWeapon, holdPoint.transform.position, holdPoint.transform.rotation, holdPoint.transform);
            existingWeapons.Add(newWeapon, newInstance);

            if (currentWeapon != null)
            {
                currentWeapon.SetActive(false);
            }
            currentWeapon = newInstance;
        }
    }

    //called from the swap animation event
    private void Finish() => onSwapFinish.Invoke();
}