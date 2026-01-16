using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class UIElement : MonoBehaviour
{
    public IEnumerator SizeFlash(float multiplier)   //1.5 for two, 1.2 for one
    {
        Vector3 orig = GetComponent<RectTransform>().localScale;

        Vector3 big = new Vector3((float)(orig.x * multiplier), (float)(orig.y * multiplier), (float)(orig.z * multiplier));

        //Vector3 obj2_big = new Vector3(0,0,0);
        //Vector3 obj2_orig = new Vector3(0, 0, 0);
        /*
        if (obj2 != null)
        {
            obj2_orig = obj2.GetComponent<RectTransform>().localScale;
            obj2_big = new Vector3((float)(obj2_orig.x * multiplier), (float)(obj2_orig.y * multiplier), (float)(obj2_orig.z * multiplier));

        }*/


        for (int i = 0; i < 5; i++)
        {
            //key.SetActive(false);
            //action.SetActive(false);
            GetComponent<RectTransform>().localScale = big;
            /*if (obj2 != null) 
            {
                obj2.GetComponent<RectTransform>().localScale = obj2_big;
            }*/
            
            yield return new WaitForSeconds(0.25f);
            //key.SetActive(true);
            GetComponent<RectTransform>().localScale = orig;
            /*if (obj2 != null)
            {
                obj2.GetComponent<RectTransform>().localScale = obj2_orig;
            }*/
            
            //action.SetActive(true);
            yield return new WaitForSeconds(0.25f);
        }

    }
/*
    public IEnumerator Flash(GameObject bar)
    {
        Vector3 bar_orig = bar.GetComponent<RectTransform>().localScale;
        Vector3 bar_big = new Vector3((float)(bar_orig.x * 1.2), (float)(bar_orig.y * 1.2), (float)(bar_orig.z * 1.2));
        for (int i = 0; i < 5; i++)
        {
            bar.GetComponent<RectTransform>().localScale = bar_big;
            yield return new WaitForSeconds(0.25f);
            bar.GetComponent<RectTransform>().localScale = bar_orig;
            yield return new WaitForSeconds(0.25f);
        }
    }
*/



}