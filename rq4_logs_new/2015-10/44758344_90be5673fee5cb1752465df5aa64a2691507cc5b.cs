using UnityEngine;
using System.Collections;

public class DragAndDrop : MonoBehaviour {

    private bool currentlyDragging;
    private Transform tankLocations;
    private GameObject foodGameObject;
    
    private bool hasSalt;

    public enum ItemType
    {
        none,
        red,
        blue
    };

    public enum SaltType
    {
        none,
        salt,
        filter
    }

    public ItemType foodType;
    public ItemType fishType;
    public ItemType filterType;
    public SaltType salt;


    void Start()
    {
        currentlyDragging = false;
        tankLocations = GameObject.Find("TankLocations").transform;
        hasSalt = false;
    }

    // Update is called once per frame
    void Update()
    {
        if (currentlyDragging)
        {
            Vector3 mousePos = Camera.main.ScreenToWorldPoint(Input.mousePosition);
            mousePos.z = 0;
            foodGameObject.transform.position = mousePos;
        }
    }

    public void PressDown()
    {
        int typeCount = 0;
        if(GetFoodType() != ItemType.none)
        {
            foodType = GetFoodType();
            typeCount++;
        } else if (GetFilterType() != ItemType.none)
        {
            filterType = GetFilterType();
            typeCount++;
        } else if (GetFishType() != ItemType.none)
        {
            fishType = GetFishType();
            typeCount++;
        } else if (GetSaltType() != SaltType.none)
        {
            salt = GetSaltType();
            typeCount++;
        }

        if(typeCount > 1)
        {
            print("ERROR: Too many types were selected");
        } else if(typeCount == 0)
        {
            print("ERROR: One type was not selected");
        }

        Vector3 mousePos = Camera.main.ScreenToWorldPoint(Input.mousePosition);
        currentlyDragging = true;
        mousePos.z = 0;
        hasSalt = false;
        if (foodType != ItemType.none)
        {
            foodGameObject = Instantiate(transform.parent.GetComponent<Items>().GetFoodGameObject(foodType), mousePos, Quaternion.identity) as GameObject;
        } else if (filterType != ItemType.none)
        {
            
        } else if(fishType != ItemType.none)
        {

        } else if(salt != SaltType.none)
        {

        }
    }

    public void PressUp()
    {
        currentlyDragging = false;
        Destroy(foodGameObject);
        foreach (Transform tank in tankLocations)
        {
            if (foodType != ItemType.none)
            {
                tank.GetComponent<FishTank>().ChangesBox(Input.mousePosition, foodType, hasSalt);
            }
        }
    }

    public ItemType GetFoodType()
    {
        return foodType;
    }

    public ItemType GetFishType()
    {
        return fishType;
    }

    public ItemType GetFilterType()
    {
        return filterType;
    }

    public SaltType GetSaltType()
    {
        return salt;
    }


}