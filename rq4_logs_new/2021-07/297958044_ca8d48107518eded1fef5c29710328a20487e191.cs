using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;

public class PropertiyListContainer : MonoBehaviour
{
    private static PropertiyListContainer _instance;
    public static PropertiyListContainer Instance { get => _instance;}
    
    [field : SerializeField]
    public float RefreshInterval { get; set; }

    [SerializeField]
    private GameObject propertyListPrefab;
    
    [SerializeField]
    private GameObject defaultText;

    private Dictionary<GameObject,GameObject> propertyLists;

    void Awake()
    {
        if (_instance != null && _instance != this)
        {
            Destroy(this.gameObject);
        }
        if (_instance == null)
        {
            _instance= this;
        }

        propertyLists = new Dictionary<GameObject, GameObject>();
    }

    public void CreatePropertyList(GameObject game)
    {
        var propertyList = Instantiate(propertyListPrefab);

        propertyList.transform.SetParent(this.transform);
        propertyList.transform.localScale = Vector3.one;
        propertyList.GetComponent<PropertyList>().ReferenceObject = game;

        propertyLists.Add(game, propertyList);

        if (defaultText.activeInHierarchy)
        {
            defaultText.SetActive(false);
        }
    }

    public void DestroyPropertyList(GameObject game)
    {
        if (propertyLists.ContainsKey(game))
        {
            Destroy(propertyLists[game]);

            propertyLists.Remove(game);
        }
        if (propertyLists.Count == 0)
        {
            defaultText.SetActive(true);
        }
    }
}