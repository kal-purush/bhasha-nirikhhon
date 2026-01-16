using UnityEngine;
using UnityEngine.UI;
using System.Collections;
using System.Collections.Generic;
using System.IO;

public class MinimapDrawer : MonoBehaviour {

    private RawImage image;
    private GameObject pathParent;
    private GameObject player;
    private List<Vector3> nodes = new List<Vector3>();

    /*

    THIS IS NOT REALLY USED, JUST WHENEVER WE CHANGE/MAKE A NEW MAP - IT JUST DRAWS PATHNODES CORRECTLY ON A BITMAP

    */

	void Start() {
        image = GetComponent<RawImage>();
        pathParent = GameObject.Find("Path");
        foreach(GameObject gm in GameObject.FindGameObjectsWithTag("PathNode")) {
            Vector3 pn = gm.GetComponent<PathNode>().getLocalPosition(pathParent.transform);
            pn = new Vector3(Mathf.RoundToInt(pn.x / 10f), 0f, Mathf.RoundToInt(pn.z / 10f));
            if(!nodes.Contains(pn))
                nodes.Add(pn);
        }
        image.texture = GenerateMinimap();
    }
    

    private Texture2D GenerateMinimap() {
        int h = 150, w = 150;
        Texture2D result = new Texture2D(w, h, TextureFormat.ARGB32, false);

        for(int x = 0; x < w; x++) {
            for(int y = 0; y < h; y++) {
                Vector3 curr = new Vector3(x, 0f, y);
                if(nodes.Contains(curr)) {
                    result.SetPixel(x, y, Color.white);
                } else {
                    result.SetPixel(x, y, Color.clear);
                }
            }
        }
        result.filterMode = FilterMode.Point;
        result.Apply();
        File.WriteAllBytes(Application.dataPath + "/MapUpdate.png", result.EncodeToPNG());
        return result;
    }
}