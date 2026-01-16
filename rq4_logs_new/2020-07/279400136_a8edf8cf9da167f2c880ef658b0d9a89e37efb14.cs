using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;

public class mirrorProperties {
    public string color;
    public string shape;
    public int area;
    public mirrorProperties(string a, string b, int c) {
        color = a;
        shape = b;
        area = c;
    }
}

public class Assign : MonoBehaviour {
    public List<mirrorProperties> CartesianProduct(string[] colors, string[] shapes, int[] areas) {
        var configs = new List<mirrorProperties>();
        var cartesianProduct = 
            from color in colors
            from shape in shapes
            from area in areas
            select new {color, shape, area};
        foreach(var pair in cartesianProduct) { 
            var tuple = new mirrorProperties(pair.color, pair.shape, pair.area);
            configs.Add(tuple);
        } 
        return configs;
    }

    public void Shuffle<T>(List<T> ls) {
        int n = ls.Count;
        while (n > 1) {
            n--;
            int i = Random.Range(0,n+1);
            var temp = ls[i];
            ls[i] = ls[n];
            ls[n] = temp;
        }
    }

    public Color StringToColor(string color) {
        if (string.Equals(color, "red")) {
            return Color.red;
        } else if (string.Equals(color, "green")) {
            return Color.green;
        } else if (string.Equals(color, "blue")) {
            return Color.blue;
        } else {
            return Color.white;
        }
    }

    public void AssignClues(mirrorProperties goalSpec, List<GameObject> cluePool1, List<GameObject> cluePool2, List<GameObject> cluePool3) {
        Random random = new Random();
        GameObject randomCluePoint1 = cluePool1[Random.Range(0,cluePool1.Count)];
        GameObject randomCluePoint2 = cluePool2[Random.Range(0,cluePool2.Count)];
        GameObject randomCluePoint3 = cluePool3[Random.Range(0,cluePool3.Count)];
        List<GameObject> cluepoints = new List<GameObject>{randomCluePoint1, randomCluePoint2, randomCluePoint3};
        Shuffle(cluepoints);
        // Assign clue objects as visible, interactable, and with the specific
        // mirror property from the goal mirror
    }
    public void AssignMirrors(int goalMirror, List<GameObject> mirrorPool, List<mirrorProperties> props) {
        for(int i = 0; i < mirrorPool.Count; i++) { 
            string color = props[i].color;
            string shape = props[i].shape;
            int area = props[i].area;
            GameObject mirrorFrame = mirrorPool[i].transform.GetChild(0).gameObject;
            var materials = mirrorFrame.GetComponent<MeshRenderer>().materials;
            Material mat = materials[0];
            mat.SetColor("_Color", StringToColor(color));
            // Assign each mirror its mirror property, indicate if that 
            // mirror is the goal mirror or not
        } 
    }
    public List<GameObject> mirrorPool;
    // public List<GameObject> cluePool1;
    // public List<GameObject> cluePool2;
    // public List<GameObject> cluePool3;

    string[] colors = {"red","blue","green"};
    string[] shapes = {"square","circle","triangle"};
    int[] areas = {1,2,3};

    void Start() { 
        var CP = CartesianProduct(colors, shapes, areas);
        int mpCount = mirrorPool.Count;
        if (mpCount > CP.Count) {
            Debug.Log("Number of mirrors is larger than the number of available combinations.");
            return; 
        }
        Debug.Log(CP.Count.ToString());
        Shuffle(CP);
        int goalMirror = Random.Range(0,mpCount);
        Debug.Log(goalMirror.ToString());
        var goalSpec = CP[goalMirror];
        AssignMirrors(goalMirror, mirrorPool, CP);
        // AssignClues(goalSpec, cluePool1, cluePool2, cluePool3);
    }
}