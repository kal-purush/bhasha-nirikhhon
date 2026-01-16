using UnityEngine;
using System.Collections;
using System;
using System.Collections.Generic;

public class NotActiveAnimals : MonoBehaviour
{
    private GameObject animalListManager = null;
    [SerializeField]
    private GameObject cage = null;

    private List<GameObject> cageList = new List<GameObject>();
    public int selectID = int.MaxValue;
    public bool is_select = false;
    public float distance = 5.0f;

    void Start()
    {
        if (cage == null)
        {
            Debug.Log("cage is empty!");
            return;
        }

        CreateCage();

        if (animalListManager == null)
        {
            Debug.Log("animalListManager is empty!");
            return;
        }

        StartCoroutine(UpdateNotActiveAnimals());
    }

    private void CreateCage()
    {
        animalListManager = GameObject.Find("AnimalListManager");
        var animalList = animalListManager.GetComponent<AnimalListManager>().animalList;
        foreach (var animal in animalList)
        {
            var animalStatus = animal.GetComponent<AnimalStatusManager>();
            if (animalStatus.status.IsPurchase == false)
                continue;
            if (animalStatus.status.CageID != 99)
                continue;

            cageList.Add(Instantiate(cage));
            cageList[cageList.Count - 1].GetComponent<CageManager>().animalID = animalStatus.status.ID;
            cageList[cageList.Count - 1].transform.localScale = new Vector3(0.75f, 0.75f, 1.0f);
            cageList[cageList.Count - 1].GetComponent<SpriteAnimationManager>().animationDataList =
                animal.GetComponent<SpriteAnimationManager>().animationDataList;
        }

        for (int i = 0; i < cageList.Count; i++)
        {
            cageList[i].transform.position =
                new Vector3(distance * (-i % 3 + 1),
                distance * (-i / 3 + 2),
                22.0f);
        }
    }

    private IEnumerator UpdateNotActiveAnimals()
    {
        while (true)
        {
            SelectAnimal();

            yield return null;
        }
    }

    private void SelectAnimal()
    {
        if (!Input.GetMouseButtonDown(0))
            return;

        foreach (var cage in cageList)
        {
            if (!cage.GetComponent<Collision>().IsHit(GetComponent<RayCollision>().HitRayPosCameraToMouse()))
                continue;

            selectID = cage.GetComponent<CageManager>().animalID;
            is_select = true;
            break;
        }

        if(!is_select)
        {
            var back = transform.GetChild(0).gameObject;

           if(back.GetComponent<Collision>().IsHit(GetComponent<RayCollision>().HitRayPosCameraToMouse()))
                is_select = true;
        }

        if (is_select)
        {
            foreach (var cage in cageList)
                Destroy(cage);
        }

    }
}