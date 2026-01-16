using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Demo1 : MonoBehaviour
{
    SceneManager m_aSceneManager = new SceneManager();
    float m_fTimeCost = 0;

    // Use this for initialization
    void Start()
    {
        // Create root node
        GameObject aRoot = new GameObject("Root");
        aRoot.transform.position = Vector3.zero;
        aRoot.transform.localScale = Vector3.one;
        aRoot.transform.rotation = Quaternion.identity;


        // Load resource & create instances
        GameObject aObj = Resources.Load("Cube") as GameObject;

        Random.seed = 0;
        for (int i = 0; i < 100000; ++i)
        {
            float x = Random.Range(-1000, 1000);
            float y = Random.Range(-1000, 1000);
            float z = Random.Range(-1000, 1000);

            SceneObject aSceneObject = new SceneObject();

            aSceneObject.m_aGameObject = GameObject.Instantiate(aObj, new Vector3(x, y, z), Quaternion.identity) as GameObject;
            aSceneObject.m_aGameObject.transform.parent = aRoot.transform;

            MeshRenderer aRender = aSceneObject.m_aGameObject.GetComponent<MeshRenderer>();
            aSceneObject.m_aBounds = aRender.bounds;

            m_aSceneManager.AddSceneObject(aSceneObject);
        }


        // test
        Bounds aBound = new Bounds(new Vector3(50.0f, 0, 50.0f), new Vector3(110.0f, 110.0f, 110.0f));

        float fTimeBegin = Time.realtimeSinceStartup;
        for (int i = 0; i < 1000; ++i)
        {
            m_aSceneManager.GetSceneObjectList(aBound);
        }
        m_fTimeCost = Time.realtimeSinceStartup - fTimeBegin;
        m_fTimeCost *= 1000;
    }


    private GUIStyle m_aGUIStyle;
    private Rect m_aGUIRect = new Rect(10, 10, 1000, 50);

    void OnGUI()
    {
        if (m_aGUIStyle == null)
        {
            m_aGUIStyle = new GUIStyle(GUI.skin.label);
            m_aGUIStyle.normal.textColor = Color.white;
            m_aGUIStyle.alignment = TextAnchor.MiddleLeft;
        }

        GUI.color = Color.green;
        GUI.Label(new Rect(0, 0, m_aGUIRect.width, m_aGUIRect.height), "TimeCost: " + m_fTimeCost.ToString() + "ms", m_aGUIStyle);
    }
}