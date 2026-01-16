using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class SlotCollector : MonoBehaviour
{
    private Image[] images = new Image[12];

	// Use this for initialization
	void Start ()
    {
	    for(int i = 0; i < 12; i += 1)
        {
            GameObject obj = transform.GetChild(i).gameObject;

            images[i] = obj.GetComponent<Image>();
        }
	}

    public void SetImagesEnable(bool ena)
    {
        for(int i = 0; i < 12; i += 1)
        {
            images[i].enabled = ena;
        }
    }
}