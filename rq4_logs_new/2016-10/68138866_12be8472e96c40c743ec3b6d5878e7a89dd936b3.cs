using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class flashanimation2 {

    public IEnumerator playBack(List<int> pattern, GameObject Red, GameObject Blue, GameObject Green, GameObject Yellow)
    {

        if (pattern != null)
        {
            for (int p = 0; p < pattern.Count; p++)
            {
                if (pattern[p] == 0)
                {
                    Red.transform.Rotate(Vector3.up, Time.deltaTime * 30, Space.World);
                    Red.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Red.transform.GetComponent<Renderer>().material.color = Color.red;
                    yield return new WaitForSeconds(0.3f);
                    continue;
                }
                else if (pattern[p] == 1)
                {
                    Green.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Green.GetComponent<Renderer>().material.color = Color.green;
                    yield return new WaitForSeconds(0.3f);
                    continue;
                }
                else if (pattern[p] == 2)
                {
                    Blue.transform.Rotate(Vector3.up, Time.deltaTime * 30, Space.World);
                    Blue.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Blue.GetComponent<Renderer>().material.color = Color.blue;
                    yield return new WaitForSeconds(0.3f);
                    continue;
                }
                else if (pattern[p] == 3)
                {
                    Yellow.GetComponent<Renderer>().material.color = Color.white;
                    yield return new WaitForSeconds(0.6f);
                    Yellow.GetComponent<Renderer>().material.color = Color.yellow;
                    yield return new WaitForSeconds(0.3f);
                    continue;

                }

               
            }
          
        }
        yield return new WaitForSeconds(0.7f);
    }
}