using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BasicWave : MonoBehaviour {


    [HideInInspector]
    public List<GameObject> entities = new List<GameObject>();

    public float TimeBetweenWave = 1.0f;
    public string TextDisplay = "Text To Configure";

    private int currentEntity = 0;

    public void Start()
    {
        for (int i = 0; i < entities.Count; i++)
        {
            GameObject instance = Instantiate(entities[i]);
            instance.transform.SetParent(this.gameObject.transform);
        }
    }

    public void AddEntity(GameObject entity)
    {
        if (!entity.GetComponent<BasicEnnemy>())
            Debug.LogError("[BasicWave] Cannot add Entity without BasicEnemy script attached");
        entities.Add(entity);
    }
	
    /// <summary>
    /// Will return the next Enenmy of the Wave
    /// </summary>
    /// <returns>GameObject</returns>
    public GameObject GetNextEnemy()
    {
        currentEntity++;
        return entities[currentEntity - 1];
    }

    public void ResetWave()
    {
        currentEntity = 0;
    }
}