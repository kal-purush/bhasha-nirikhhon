using UnityEngine;

public class TangentSpaceViz: MonoBehaviour{

    [SerializeField]
    float lineScale = 1f;
    [SerializeField] 
    float lineOffset = 0f;
    void OnDrawGizmos()
    {
        if (!enabled){
            return;
        }
        MeshFilter mf;
        if(TryGetComponent(out mf)){
            Mesh m = mf.sharedMesh;
            
            Vector3[] verts = m.vertices;
            Vector3[] normals = m.normals;
            Vector4[] tans = m.tangents;
    
            for (int i =0; i < verts.Length; i++){

                DrawTangentSpace(
                    transform.TransformPoint(verts[i]), 
                    transform.TransformDirection(normals[i]),
                    transform.TransformDirection(tans[i]),
                    tans[i].w);
            }
        }else{
            Debug.Log("Did not find mesh");
            this.enabled = false;
        }
    }

    void DrawTangentSpace(Vector3 v, Vector3 n, Vector3 t, float tanSign ){
        Vector3 start = v + n*lineOffset;
        Gizmos.color = Color.green; // Up direction in tangent space
        Gizmos.DrawLine(start,start + n*lineScale );
        Gizmos.color = Color.red; // right dir in tan space
        Gizmos.DrawLine(start, start + t * lineScale);
        Vector3 b = Vector3.Cross(n, t) * tanSign;
        Gizmos.color = Color.blue; // forward in tan space
        Gizmos.DrawLine(start, start + b * lineScale);
    }
}