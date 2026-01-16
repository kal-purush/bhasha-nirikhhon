using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
public class ChestManager : MonoBehaviour
{
    public List<string> ListName;
    public List<int> ListCount;
    public List<GameObject> ListObj;

    public List<Button> but;

    public Button Clone;
    public GameObject content;
    public static bool SecondInventoy;
    // Start is called before the first frame update
    void Start()
    {
        ListName.Clear();
        ListCount.Clear();
        ListObj.Clear();
        SecondInventoy = false;
    }

    // Update is called once per frame
    
    public static void Chset()
    {
        SecondInventoy = true;
    }
    public void ListCerate()
    {
        but.Clear();
        // Debug.Log(string.Join(",", but.Select(but => but.ToString())));


        //    int i = 1;
        //    foreach (string a in _InventoryList.name)
        //    {
        //        RectTransform posi = CloneButton.GetComponent<RectTransform>();
        //        CloneButton = (Button)Instantiate(CloneButton_) as Button;//(i+10+interval)*-1
        //        CloneButton.transform.parent = content.transform;
        //        CloneButton.GetComponent<RectTransform>().anchoredPosition = new Vector2(0, (-(i + 1) * 30));

        //        text = CloneButton.gameObject.transform.FindChild("clonetext").gameObject;
        //        Text _text = text.gameObject.GetComponent<Text>();
        //        int b;
        //        b = _InventoryList.count[i - 1];
        //        _text.text = a + " �~" + b;
        //        i++;
        //        but.Add(CloneButton);

        //        buttanData data = CloneButton.GetComponent<buttanData>();
        //        data.name = a;
        //        data.count = b;
        //        data.obj = _InventoryList.obj[i];


        //    }

        int i = 0;

        int C = ListName.Count;

        for (i = 0; i <= C + 1; i++)
        {
            string a = ListName[i];
            Button cloneButton = Instantiate(Clone) as Button;
            cloneButton.transform.SetParent(content.transform, false);
            cloneButton.GetComponent<RectTransform>().anchoredPosition = new Vector2(0, -(i + 1) * 30);

            GameObject textObj = cloneButton.transform.Find("clonetext").gameObject;
            Text _text = textObj.GetComponent<Text>();

            int b = ListCount[i];
            _text.text = a + " �~" + b;

            but.Add(cloneButton);

            buttanData data = cloneButton.GetComponent<buttanData>();
            data.number = i;
            // data.obj = _InventoryList.obj[i];
            // Debug.Log(i + "  " + C);
        }
    }
    public void DestroyButton()
    {
        //// Debug.Log("1  " +(string.Join(",", but.Select(but => but.ToString()))));

        foreach (Button a in but)
        {
            Destroy(a.gameObject);

        }
        //   Debug.Log("2  "+(string.Join(",", but.Select(but => but.ToString()))));
    }   //but = new List&It;string&gt();
}