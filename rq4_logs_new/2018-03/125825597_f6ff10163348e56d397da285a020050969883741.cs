using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ApplicationManager : MonoBehaviour {

    //public:
    public static ApplicationManager AM;
//    public List<Lab> m_Labs;
    public SceneData[] m_Scenes;

    [HideInInspector]
    public string m_PreviousScene = "";
    [HideInInspector]
    public string m_CurrentScene= "";
    [HideInInspector]
    public int m_CurrentScenesIndex = 0;
    [HideInInspector]
    public int m_SceneToLoadIndex = -1;

    // Use this for initialization
    void Awake () {
        if (AM == null)
        {
            DontDestroyOnLoad(gameObject);
            AM = this;
        }
        else if (AM != this)
        {
            Destroy(gameObject);
        }

        if (m_Scenes != null)
        {
            m_CurrentScene = m_Scenes[0].m_SceneName;
        }
	}

    public void fn_TransferSceneData(string newScene)
    {
        //Tested
        for (int i = 0; i < m_Scenes.Length; i++)
        {
            if (m_Scenes[i].m_SceneName == newScene)
            {
                m_PreviousScene = m_CurrentScene;
                m_CurrentScene = newScene;
                m_CurrentScenesIndex = i;
                m_SceneToLoadIndex =  m_Scenes[i].m_SceneIndex;
                return;
            }
        }
    }
}

[System.Serializable]
public struct SceneData
{
    public string m_SceneName;
    public int m_SceneIndex;
    public bool isLabScene;
    public ToolType[] m_RequiredTools;
    public Mission[] m_Missions;
    public int LastMissionIndex;
}

[System.Serializable]
public struct Mission
{
    public string m_MissionDescription;
    public ToolType m_CurrentNeededTool;
    public ToolType m_NextNeededTool;
    public LabState m_ExpectedAction;
    [HideInInspector]
    public bool isDone;

}

public enum ToolType {Beaker, Bunsen_Burner, Dropper, Container_Sample, MircoScope_GlassSection, MircoScope, Mortar_Pestle, Scalple, TestingTubes_Rack, TestingTube, Ice,
Thermometer, Tongs, Iodine_Solution, Glucose_Solution, Starch_Solution, Egg_Yolk, Distilled_Water, Benedict_Reagent, Beans_Solution, Peanut_Solution, Peanut_Reagent, Biuret_Reagent,
Potatos, Injection, Scalpel, Peas_Seeds_Container, Tomatoes_Container, Wheat_Seeds_Container, Bread_Pieces_Container, Water, Elodea, Drying_Paper, Cover_Glass, Lab_Palette, Onion, Combinator
}