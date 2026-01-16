using UnityEngine;
using System.Collections;
using VolumetricLines;
public class CreateBeamLine : MonoBehaviour
{
	public Material m_volumetricLineStripMaterial;
	public Color m_color;
    public Vector3 m_startPos;
    public Vector3 m_endPos;
	public float m_lineWidth;

    private GameObject go;
    private VolumetricLineStripBehavior volLineStrip;
    private Vector3[] lineVertices;

    // public VolumetricLineStripBehavior volLineStrip;

	// Use this for initialization
    void Start(){
        // Create an empty game object
		go = new GameObject();
		go.transform.parent = transform;

		// Add the MeshFilter component, VolumetricLineStripBehavior requires it
		go.AddComponent<MeshFilter>();

		// Add a MeshRenderer, VolumetricLineStripBehavior requires it
		go.AddComponent<MeshRenderer>();

		// Add the VolumetricLineStripBehavior and set parameters, like color and all the vertices of the line
		volLineStrip = go.AddComponent<VolumetricLineStripBehavior>();
		volLineStrip.DoNotOverwriteTemplateMaterialProperties = false;
		volLineStrip.TemplateMaterial = m_volumetricLineStripMaterial;
		volLineStrip.LineColor = m_color;
        volLineStrip.LineWidth = m_lineWidth;
        volLineStrip.LightSaberFactor = 0.83f;

        lineVertices = new Vector3[3];
        lineVertices[0] = gameObject.transform.TransformPoint(m_startPos);
        lineVertices[1] = gameObject.transform.TransformPoint((m_startPos + m_endPos) / 2.0f);
        lineVertices[2] = gameObject.transform.TransformPoint(m_endPos);

		volLineStrip.UpdateLineVertices(lineVertices);
    }

	void Update () 
	{
		volLineStrip.LineWidth = m_lineWidth;
        lineVertices[0] = gameObject.transform.TransformPoint(m_startPos);
        lineVertices[1] = gameObject.transform.TransformPoint((m_startPos + m_endPos) / 2.0f);
        lineVertices[2] = gameObject.transform.TransformPoint(m_endPos);

		volLineStrip.UpdateLineVertices(lineVertices);

	}
}