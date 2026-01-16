using UnityEngine;
using Unity.AI.Navigation;
using UnityEngine.AI;
using System.Collections.Generic;


namespace SplashEdit.RuntimeCode
{
    public struct PSXNavMeshTri
    {
        public PSXNavmeshVertex v0, v1, v2;
    }

    public struct PSXNavmeshVertex
    {
        public short vx, vy, vz;
    }

    [RequireComponent(typeof(NavMeshSurface))]
    public class PSXNavMesh : MonoBehaviour
    {

        [HideInInspector]
        public List<PSXNavMeshTri> Navmesh { get; set; }

        public void CreateNavmesh(float GTEScaling)
        {
            Navmesh = new List<PSXNavMeshTri>();
            NavMeshSurface navMeshSurface = GetComponent<NavMeshSurface>();
            navMeshSurface.BuildNavMesh();
            NavMeshTriangulation triangulation = NavMesh.CalculateTriangulation();

            int[] triangles = triangulation.indices;
            Vector3[] vertices = triangulation.vertices;

            for (int i = 0; i < triangles.Length; i += 3)
            {


                int vid0 = triangles[i];
                int vid1 = triangles[i + 1];
                int vid2 = triangles[i + 2];

                PSXNavMeshTri tri = new PSXNavMeshTri();

                tri.v0.vx = PSXTrig.ConvertCoordinateToPSX(vertices[vid0].x, GTEScaling);
                tri.v0.vy = PSXTrig.ConvertCoordinateToPSX(-vertices[vid0].y, GTEScaling);
                tri.v0.vz = PSXTrig.ConvertCoordinateToPSX(vertices[vid0].z, GTEScaling);

                tri.v1.vx = PSXTrig.ConvertCoordinateToPSX(vertices[vid1].x, GTEScaling);
                tri.v1.vy = PSXTrig.ConvertCoordinateToPSX(-vertices[vid1].y, GTEScaling);
                tri.v1.vz = PSXTrig.ConvertCoordinateToPSX(vertices[vid1].z, GTEScaling);

                tri.v2.vx = PSXTrig.ConvertCoordinateToPSX(vertices[vid2].x, GTEScaling);
                tri.v2.vy = PSXTrig.ConvertCoordinateToPSX(-vertices[vid2].y, GTEScaling);
                tri.v2.vz = PSXTrig.ConvertCoordinateToPSX(vertices[vid2].z, GTEScaling);

                Navmesh.Add(tri);
            }
        }
    }
}