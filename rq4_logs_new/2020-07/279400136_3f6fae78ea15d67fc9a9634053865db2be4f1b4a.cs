using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LoseLooping : MonoBehaviour
{
    [SerializeField]
    private GameObject leftCorridors;
    [SerializeField]
    private GameObject rightCorridors;
    [SerializeField]
    private GameObject current;

    private GameObject allCorridors;
    private float xOffset;

    private const string LEFT = "Left";
    private const string RIGHT = "Right";
    private const string CURRENT = "Current";

    private void Start()
    {
        allCorridors = this.gameObject;
        Transform t = leftCorridors.transform.GetChild(0);
        xOffset = Mathf.Abs(t.position.x);
    }

    public void HandleMovement(GameObject newCurrent)
    {
        if(newCurrent.tag == LEFT)
        {
            current.transform.SetParent(rightCorridors.transform);
            current.transform.SetAsFirstSibling();
            current.tag = RIGHT;

            newCurrent.tag = CURRENT;
            newCurrent.transform.SetParent(allCorridors.transform);
            current = newCurrent;

            GameObject toMove = rightCorridors.transform.GetChild(rightCorridors.transform.childCount - 1).gameObject;
            toMove.tag = LEFT;
            toMove.transform.SetParent(leftCorridors.transform);
            toMove.transform.SetAsLastSibling();
            int shiftBy = leftCorridors.transform.childCount + rightCorridors.transform.childCount + 1;
            Vector3 toMovePos = toMove.transform.position;
            toMove.transform.position = new Vector3(toMovePos.x - shiftBy * xOffset, toMovePos.y, toMovePos.z);
        } else if(newCurrent.tag == RIGHT)
        {
            current.transform.SetParent(leftCorridors.transform);
            current.transform.SetAsFirstSibling();
            current.tag = LEFT;

            newCurrent.tag = CURRENT;
            newCurrent.transform.SetParent(allCorridors.transform);
            current = newCurrent;

            GameObject toMove = leftCorridors.transform.GetChild(leftCorridors.transform.childCount - 1).gameObject;
            toMove.tag = RIGHT;
            toMove.transform.SetParent(rightCorridors.transform);
            toMove.transform.SetAsLastSibling();
            int shiftBy = leftCorridors.transform.childCount + rightCorridors.transform.childCount + 1;
            Vector3 toMovePos = toMove.transform.position;
            toMove.transform.position = new Vector3(toMovePos.x + shiftBy * xOffset, toMovePos.y, toMovePos.z);

        }
    }


}