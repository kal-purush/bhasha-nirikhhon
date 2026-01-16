using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DeleteButton : MonoBehaviour {


    public GameObject toDelete;
    public GameObject Hand;

    private void OnMouseDown()
    {
        if (toDelete != null)
        {
            //better change these two lines below - working on public variables
            GetComponent<Transform>().Find("/Main Camera/HexUI").gameObject.GetComponent<MovingObject>().ActualNumberToDraw--;
            GetComponent<Transform>().Find("/Main Camera/HexUI").gameObject.GetComponent<MovingObject>().numberOfDrawnHex--;

            //move to gravyard and remove mose of components
            toDelete.GetComponent<Transform>().parent = GetComponent<Transform>();
            toDelete.GetComponent<Transform>().position = new Vector3(GetComponent<Transform>().position.x, GetComponent<Transform>().position.y, GetComponent<Transform>().position.z + 0.01f);
            Destroy(toDelete.GetComponent<Rigidbody2D>());
            Destroy(toDelete.GetComponent<mouseOverHex>());
            Destroy(toDelete.GetComponent<ToRemove>());
            Destroy(toDelete.GetComponent<PolygonCollider2D>());

            toDelete = null;
        }
        sortHex();
    }

    void sortHex()
    {
        Hand = GetComponent<Transform>().Find("/Main Camera/HexUI/HandBG").gameObject;

        List<GameObject> HandList = new List<GameObject>();
        for( int i = 0; i < Hand.GetComponent<Transform>().childCount; ++i)
        {
            HandList.Add(Hand.GetComponent<Transform>().GetChild(i).gameObject);
        }
        for( int i = 0; i < HandList.Count - 1; ++i)
        {
            for (int j = 0; j < HandList.Count - 1; ++j)
            {
                if (HandList[j].GetComponent<Transform>().childCount == 0 && HandList[j + 1].GetComponent<Transform>().childCount != 0) 
                {
                    HandList[j + 1].GetComponent<Transform>().GetChild(0).GetComponent<Transform>().position = new Vector3(HandList[j].GetComponent<Transform>().position.x,
                        HandList[j].GetComponent<Transform>().position.y, HandList[j].GetComponent<Transform>().position.z - 0.1f);
                    HandList[j + 1].GetComponent<Transform>().GetChild(0).GetComponent<Transform>().parent = HandList[j].GetComponent<Transform>();
                }
        }

        }
    }
    
}