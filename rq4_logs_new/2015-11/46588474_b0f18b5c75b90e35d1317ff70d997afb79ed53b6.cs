using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class BlockFacotry : MonoBehaviour {

    [SerializeField]
    protected GameObject wallPrefab;
    protected List<GameObject> walls;

    public static BlockFacotry instance;

    public static BlockFacotry Instance
    {
        get
        { return instance; }
    }

	// Use this for initialization
	void Start () {
        if (instance == null)
        {
            instance = this;
            GameObject tmp;
            // les murs
            walls = new List<GameObject>();
            int i;
            for (i = 0; i < 10; i++)
            {
                tmp = Instantiate(wallPrefab);
                tmp.transform.SetParent(this.transform);
                tmp.SetActive(false);
                walls.Add(tmp);
            }
        }
	}
	
	// Update is called once per frame
	void Update () {
	
	}

    /* renvoie un mur non active*/
    public GameObject RequestWall()
    {
        GameObject ret;
        ret = walls[walls.Count - 1];
        walls.RemoveAt(walls.Count - 1);
        return ret;
    }

    public void GiveWall(GameObject wall)
    {
        wall.transform.SetParent(this.transform);
        wall.SetActive(false);
        walls.Add(wall);
    }


}
