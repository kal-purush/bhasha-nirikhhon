using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.IO;
using System;
using System.Collections.Generic;

public class SaveSystem : MonoBehaviour
{
    public TextAsset JSON;

    public MeshBoneLoader meshBoneLoader;

    //public int[] arr = new int[10];

    [System.Serializable]
    public class Vertex
    {
        public int vertIndex;
        public int bouneIndex;
        public int waight;

        public Vertex()
        {
            vertIndex = 0;
            bouneIndex = 0;
            waight = 0;
        }
    }

    [System.Serializable]
    public class VertexList 
    {
        public int countList = 0;
        //public Vertex[] list;
        public Vertex[] list;

        public int size = 30;

        public VertexList()
        {
            list = new Vertex[size];
            for (int i = 0; i < size; i++)
            {
                //Vertex newV = new Vertex();
                //newV.vertIndex = i;

                list[i] = new Vertex();
                list[i].vertIndex = i;
                //Debug.Log(list[i].vertIndex);
                countList++;
            }
        }

    }

    VertexList vertexList = new VertexList();


    // Start is called before the first frame update
    void Start()
    {

        //vertexList.list[0] = new Vertex();

        //arr[0] = 1;

        //AddVertexToList(1, 1, 1);
    }

    // Update is called once per frame
    void Update()
    {
        //vertexList = JsonUtility.FromJson<VertexList>(JSON.text);
    }

    public void WriteTOJSON()
    {
        string strOutput = JsonUtility.ToJson(vertexList);

        File.WriteAllText(Application.dataPath + "/JSON.txt", strOutput);
    }

    public void ClearSaves()
    {
        string strOutput = "";

        File.WriteAllText(Application.dataPath + "/JSON.txt", strOutput);

        Array.Clear(vertexList.list, 0, vertexList.list.Length);

    }

    public void AddVertexToList(int vertexInd, int boneInd, int weight)
    {
        //Debug.Log(vertexList.countList);
        //vertexList.list[0] = new Vertex();
        //Vertex newVert = new Vertex();
        vertexList.list[vertexInd].vertIndex = vertexInd;
        vertexList.list[vertexInd].bouneIndex = boneInd;
        vertexList.list[vertexInd].waight = weight;


        //int cunt2 = vertexList.count;
        //vertexList.list[vertexInd] = newVert;
        //vertexList.countList++;
        //Debug.Log(vertexList.count);

        //List<int>[] arr = new List<int>;



    }

    public void LoadFormJSON()
    {
        vertexList = JsonUtility.FromJson<VertexList>(JSON.text);

        meshBoneLoader.LoadFormJSONFile();
    }

    public Vertex getVertex(int vertInd)
    {
        return vertexList.list[vertInd];
    }
}