using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEditor;

public class InstantMesh : MonoBehaviour
{
    [SerializeField] GameObject[] meshPointPos = new GameObject[5];   //メッシュの頂点用
    [SerializeField] Material material;                               //作成したメッシュのマテリアル
    [SerializeField] float length = 5;
    [SerializeField] float width = 5;
    [SerializeField] float height = 5;

    [Header("メッシュを書き出すボタン")]
    [SerializeField] bool exportMeshTrigger;

    [Header("メッシュを書き出す場所")]
    [SerializeField] string exportMeshLink = "Assets/Pyramid.asset";

    GameObject myMeshObj;                                             //動的メッシュの器      
    MeshFilter myMeshObjMesh;

    // Start is called before the first frame update
    void Start()
    {
        //動的メッシュ作成用オブジェクト準備
        myMeshObj = new GameObject("test");
        myMeshObj.AddComponent<MeshRenderer>();
        myMeshObjMesh = myMeshObj.AddComponent<MeshFilter>();
        myMeshObj.GetComponent<Renderer>().material = material;
        myMeshObj.transform.parent = transform;

        width /= 2;
        height /= 2;

        //頂点座標を設定
        for (int i = 0; i < meshPointPos.Length; i++)
        {
            meshPointPos[i] = GameObject.CreatePrimitive(PrimitiveType.Cube);
            Vector3 pos = Vector3.one;

            switch (i)
            {
                case 0: pos = new Vector3(0, 0, 0); break;
                case 1: pos = new Vector3(-width, -height, length); break;
                case 2: pos = new Vector3(-width, height, length); break;
                case 3: pos = new Vector3(width, height, length); break;
                case 4: pos = new Vector3(width, -height, length); break;
                default: break;
            }

            meshPointPos[i].transform.position = pos;
            meshPointPos[i].transform.parent = transform;

        }

        //メッシュ作成
        Vector3[] posArray = new Vector3[meshPointPos.Length];
        for (int i = 0; i < meshPointPos.Length; i++)
        {
            posArray[i] = meshPointPos[i].transform.position;
        }
        myMeshObjMesh.sharedMesh = MeshCreate(posArray);
    }

    // Update is called once per frame
    void Update()
    {
       

        //メッシュの書き出し
        if (exportMeshTrigger)
        {
            AssetDatabase.CreateAsset(myMeshObjMesh.sharedMesh, exportMeshLink);
            AssetDatabase.SaveAssets();
            exportMeshTrigger = false;
        }
    }

    //動的メッシュの作成
    Mesh MeshCreate(Vector3[] pos)
    {
        var mesh = new Mesh();
        mesh.vertices = new Vector3[]   //メッシュの頂点座標
        {
            pos[0], pos[1], pos[2],
            pos[0], pos[2], pos[3],
            pos[0], pos[3], pos[4],
            pos[0], pos[4], pos[1],
            pos[1], pos[4], pos[2],
            pos[2], pos[4], pos[3]
        };
        mesh.triangles = new int[]      //パネルの向き
        {
            0, 1, 2,
            3, 4, 5,
            6, 7, 8,
            9, 10, 11,
            12, 13, 14,
            15, 16, 17
        };
        mesh.uv = new Vector2[]        //UV展開、これをしないと一色のマテリアルは大丈夫だけど、複雑な模様だとちゃんと反映されない
        {                              //内部の数が大きければ多いほど模様が細かくなる
            new Vector2 (0.5f, 1f),new Vector2 (1f, 0), new Vector2 (0, 0),
            new Vector2 (0.5f, 1f),new Vector2 (1f, 0), new Vector2 (0, 0),
            new Vector2 (0.5f, 1f),new Vector2 (1f, 0), new Vector2 (0, 0),
            new Vector2 (0.5f, 1f),new Vector2 (1f, 0), new Vector2 (0, 0),
            new Vector2 (0.5f, 1f),new Vector2 (1f, 0), new Vector2 (0, 0),
            new Vector2 (0.5f, 1f),new Vector2 (1f, 0), new Vector2 (0, 0),
        };

        mesh.RecalculateNormals();     //おまじない
        return mesh;
    }
}