using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Container : MonoBehaviour {

    //public Hashtable itemKeys = new Hashtable();
    public TextAsset allItems;
    public string[] possibleItems;
    public int[] numberOfIndItems;
    public int maxItems, currentNumItems;
    public string currentItem;
    bool resetAllValues;
	// Use this for initialization
	void Start () {
        //Initialize the inventory array for the item, and their total numbers
		if(allItems != null)
        {
            possibleItems = allItems.text.Split('\n');
            numberOfIndItems = new int[possibleItems.Length];
            /*for( int i = 0; i < possibleItems.Length; i++)
            {
                numberOfIndItems[i] = 0;
            }*/

        }
	}
	
	// Update is called once per frame
	void Update () {
        /*if (Input.GetKeyDown(KeyCode.Space))
        {
            ejectFromContainer(this.gameObject);
        }*/
	}

    void OnCollisionEnter(Collision collision)
    {
        //Debug.Log("Does a collision happen");
        if(collision.gameObject.GetComponent<BaseItem>() != null)
        {
            currentItem = collision.gameObject.GetComponent<BaseItem>()._NAME;
            int tempIndex = getIndex(currentItem);
            Debug.Log(tempIndex);
            if(tempIndex != -1)
            {
                numberOfIndItems[tempIndex] += 1;
                Destroy(collision.gameObject);
            }else
            {
                Debug.Log("Wrong Index");
                ejectFromContainer(collision.gameObject);
            }
        }else
        {
            Debug.Log("Wrong File");
            ejectFromContainer(collision.gameObject);
        }
        
    }

    //eject the object from the container
    public void ejectFromContainer(GameObject theObject)
    {
        Vector3 thrust = new Vector3(Random.Range(-90.0f, 90.0f), 450, Random.Range(-90.0f, 90.0f));
        Debug.Log(transform.up);
        theObject.GetComponent<Rigidbody>().AddForce(Vector3.Scale(new Vector3(1,1,1), thrust));
    }
    /// <summary>
    /// returns the index of the object in the possibleItems array
    /// </summary>
    /// <param name="InvObj"></param>
    /// <returns></returns>
    public int getIndex(string InvObj)
    {
        //Debug.Log(InvObj);
        for(int i = 0; i < possibleItems.Length; i++)
        {
            if (possibleItems[i].Trim().Equals(InvObj.Trim()))
            {
                Debug.Log(i);
                return i;
            }
        }
        return -1;
    }
}