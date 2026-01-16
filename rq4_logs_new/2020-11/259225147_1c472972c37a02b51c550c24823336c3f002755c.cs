using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TitleMenuCamera : MonoBehaviour
{
    [SerializeField] GameObject camera;
    [SerializeField] GameObject selectMenuIconGroup;
    [SerializeField] Transform pivot;
    [SerializeField] float speed = 3f;

    int listNum = 0;
    List<GameObject> select = new List<GameObject>();

    // Start is called before the first frame update
    void Start()
    {
        var s = selectMenuIconGroup.GetComponentsInChildren<TitleMenuSelectIcon>();
        foreach(var obj in s)
        {
            select.Add(obj.gameObject);
        }
    }

    // Update is called once per frame
    void Update()
    {
        if (IsListNumPlusMove())
        {
            if (listNum < select.Count - 1) listNum++;
            else listNum = 0;
        }
        else if (IsListNumMinusMove())
        {
            if (listNum > 0) listNum--;
            else listNum = select.Count - 1;
        }


        transform.LookAt(pivot);
        var target = select[listNum].transform.position;
        transform.position = Vector3.Lerp(transform.position, target, Time.deltaTime * speed);
    }

    bool IsLimit(int target ,int max)
    {
        Debug.Log(listNum.ToString());
        return target >= 0 && target < max;
    }

    bool IsListNumPlusMove()
    {
        return Input.GetKeyDown(KeyCode.LeftArrow);
    }

    bool IsListNumMinusMove()
    {
        return Input.GetKeyDown(KeyCode.RightArrow);
    }
}