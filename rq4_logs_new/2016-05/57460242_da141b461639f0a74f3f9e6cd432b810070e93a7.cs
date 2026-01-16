using UnityEngine;
using System.Collections.Generic;
using System;
using System.Linq;

public class Light : MonoBehaviour
{
    BoxCollider2D[] colliders;
    public LayerMask mask;

    public Material myMaterial;


    private List<Vector2> cornerPoints = new List<Vector2>();
    private GameObject myObject;
    private List<Vector3> nodePositions;
    private List<Vector3> sortedPos;
    private Vector2 objectLocation;


    // Use this for initialization
    void Start()
    {
        myObject = new GameObject();
        myObject.AddComponent<MeshFilter>();
        myObject.AddComponent<MeshRenderer>();

        colliders = FindObjectsOfType<BoxCollider2D>();

        updateColliders();
    }

    void Update()
    {

        objectLocation = gameObject.transform.position;
        updateColliders();
        shootRays(objectLocation);
        updatePolygon();
        
    }


    private void updateColliders()
    {
        cornerPoints.Clear();
        foreach (BoxCollider2D i in colliders)
        {
            if (1 << i.gameObject.layer == mask.value)
            {
                Vector2 size = i.GetComponent<Transform>().lossyScale;
                Vector3 centerpoint = new Vector3(i.GetComponent<Collider2D>().offset.x,
                                                     i.GetComponent<Collider2D>().offset.y,
                                                     0f);
                Vector3 worldPos = i.transform.TransformPoint(i.GetComponent<Collider2D>().offset);

                float top = worldPos.y + (size.y / 2f);
                float btm = worldPos.y - (size.y / 2f);
                float left = worldPos.x - (size.x / 2f);
                float right = worldPos.x + (size.x / 2f);


                cornerPoints.Add(new Vector3(left, btm, worldPos.z));
                cornerPoints.Add(new Vector3(right, btm, worldPos.z));
                cornerPoints.Add(new Vector3(right, top, worldPos.z));
                cornerPoints.Add(new Vector3(left, top, worldPos.z));

            }
        }
    }


    private void shootRays(Vector3 pos)
    {
        nodePositions.Clear();

        for (int i = 0; i < cornerPoints.Count; i++)
        {
            RaycastHit2D hit = Physics2D.Linecast(objectLocation, cornerPoints[i], 1 << LayerMask.NameToLayer("Light"));


            RaycastHit2D hit2 = Physics2D.Linecast(objectLocation, cornerPoints[i] + new Vector2(0.1f, 0.1f), 1 << LayerMask.NameToLayer("Light"));
            RaycastHit2D hit3 = Physics2D.Linecast(objectLocation, cornerPoints[i] - new Vector2(0.1f, 0.1f), 1 << LayerMask.NameToLayer("Light"));

            if (hit == true)
            {
                //createLine(pos, hit.point, i);
                nodePositions.Add(hit.point);
            }
            if (hit2 == true)
            {
                nodePositions.Add(hit2.point);
            }
            if (hit3 == true)
            {
                nodePositions.Add(hit3.point);
            }
        }

    }

    public void updatePolygon()
    {
        //Destroy old game object
        myObject.GetComponent<MeshFilter>().mesh.Clear();



        //New mesh and game object
        myObject.name = "MousePolygon";
        Mesh mesh = new Mesh();

        //Components
        MeshFilter MF = myObject.GetComponent<MeshFilter>();
        MeshRenderer MR = myObject.GetComponent<MeshRenderer>();
        //myObject[x].AddComponent();

        //Create mesh
        mesh = CreateMesh(0);

        //Assign materials
        MR.material = myMaterial;

        //Assign mesh to game object
        MF.mesh = mesh;
        myObject.GetComponent<Transform>().Translate(new Vector3(0, 0, 1f));
    }



    private Mesh CreateMesh(int num)
    {

        int x; //Counter

        //Create a new mesh
        Mesh mesh = new Mesh();

        //Vertices

        // Remove duplicate vectors
        List<Vector3> noDupes = nodePositions.Distinct().ToList<Vector3>();
        nodePositions = noDupes;
        var vertex = new Vector3[nodePositions.Count];

        for (x = 0; x < nodePositions.Count; x++)
        {
            vertex[x] = nodePositions[x];
        }

        // Sort vectors to be rendered in a clockwise order
        Vector2Sort sorter = new Vector2Sort(objectLocation);
        Array.Sort(vertex, sorter.Compare);
        // Put them in a temporary list
        sortedPos.Clear();
        for (x = 0; x < vertex.Length; x++)
        {
            sortedPos.Add(vertex[x]);
        }
        // Insert the mouse location as the first vector
        sortedPos.Insert(0, objectLocation);
        // add the updated list back into the array
        for (x = 0; x < nodePositions.Count; x++)
        {
            vertex[x] = sortedPos[x];
        }

        //UVs
        var uvs = new Vector2[vertex.Length];

        for (x = 0; x < vertex.Length; x++)
        {
            if ((x % 2) == 0)
            {
                uvs[x] = new Vector2(0, 0);
            }
            else
            {
                uvs[x] = new Vector2(1, 1);
            }
        }

        //Triangles
        var tris = new int[3 * (vertex.Length - 2) + 3];    //3 verts per triangle * num triangles, plus we need additional polygon to tie them up
        int C1;
        int C2;
        int C3;


        C1 = 0;
        C2 = 1;
        C3 = 2;
        // everything except the last polygon that will tie first and last nodes
        for (x = 0; x < tris.Length - 3; x += 3)
        {
            tris[x] = C1;
            tris[x + 1] = C2;
            tris[x + 2] = C3;

            C2++;
            C3++;
        }
        // tie up the last and first node if the polygon is larger than 3
        if (tris.Length > 3)
        {
            tris[tris.Length - 3] = 0;
            tris[tris.Length - 2] = vertex.Length - 1;
            tris[tris.Length - 1] = 1;
        }



        //Assign data to mesh
        mesh.vertices = vertex;
        mesh.uv = uvs;
        mesh.triangles = tris;

        //Recalculations
        mesh.RecalculateNormals();
        mesh.RecalculateBounds();
        mesh.Optimize();

        //Name the mesh
        mesh.name = "MyMesh";

        //Return the mesh
        return mesh;
    }

}