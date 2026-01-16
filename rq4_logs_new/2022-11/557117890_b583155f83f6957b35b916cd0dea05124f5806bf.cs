using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class align_script : MonoBehaviour
{
    public GameObject mainRoom, fakeRoom;
    public GameObject player;

    [SerializeField] public GameObject[] startingStair;
    [SerializeField] public GameObject[] destinationStair;
    [SerializeField] public int[] rotateBy;
    


    private GameObject currAlignment;
    // Start is called before the first frame update
    void Start()
    {
        currAlignment = startingStair[0];
    }

    // Update is called once per frame

    public void checkToAlign(Vector3 _target)
    {
        float closestDistance = Vector3.Distance(player.transform.position,currAlignment.transform.position);
        GameObject closestObject = currAlignment;
        foreach (var stair in startingStair)
        {
            float curr = Vector3.Distance(player.transform.position,stair.transform.position);
            if (curr < closestDistance)
            {
                closestObject = stair;
                closestDistance = curr;
            }
            if (closestObject != currAlignment)
            {
            
                currAlignment = closestObject;
                int newIndex = System.Array.IndexOf(startingStair,currAlignment);
                align(startingStair[newIndex].transform,destinationStair[newIndex].transform,rotateBy[newIndex]);
            }

        }
    }




    void align(Transform _alignTo, Transform _toAlign, int _rotate)
    {

        //fakeRoom.transform.position = Vector3.zero;

        fakeRoom.transform.rotation = new Quaternion(0,0,0,1);
        fakeRoom.transform.Rotate(new Vector3(0,1,0), Quaternion.Angle(_toAlign.localRotation, _alignTo.localRotation) + _rotate);
        fakeRoom.transform.position = new Vector3(0,0,0);
//
        //fakeRoom.transform.rotation = Quaternion.LookRotation(_alignTo.position - _toAlign.position,Vector3.up);
        Vector3 to_trans = _alignTo.position - _toAlign.position ;
        fakeRoom.transform.position = to_trans;


    }
}