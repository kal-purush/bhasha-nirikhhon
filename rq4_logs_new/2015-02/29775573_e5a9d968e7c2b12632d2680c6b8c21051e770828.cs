using UnityEngine;
using System.Collections;
using System.IO;
using System.Collections.Generic;
using System.Xml;

public class ActionsData : MonoBehaviour {

    private static Dictionary<string, string> actions = new Dictionary<string, string>()
    { 
        {"US", @"<actions>
                    <scene id='Menu'>
                        <events>
                            <event name='start'>START</event>
                            <event name='credits'>CREDITS</event>
                            <event name='back'>BACK</event>
                        </events>                    
                        </scene>
                    <scene id='Cen01_Quarto'>
                        <events>
                            <event name='cat'>Pet kitty</event>
                            <event name='door'>Leave bedroom</event>
                        </events>
                    </scene>
                    <scene id='Cen02_Cozinha'>
                        <events>
                            <event name='door'>Go to living room</event>
                            <event name='geladeira'>Open fridge</event>
                        </events>
                    </scene>
                    <scene id='Cen03_Sala'>
                        <events>
                            <event name='dad'>Talk to dad</event>
                            <event name='dad_remote'>Give remote</event>
                            <event name='dad_decision'>TAKE IT                      IGNORE</event>
                            <event name='remote'>Take the remote</event>
                            <event name='door'>Go work</event>
                        </events>
                    </scene>
                    <scene id='Cen05_Parque'>
                        <events>
                            <event name='door'>Go back home</event>
                            <event name='churrosSeller'>Ask for a churros</event>
                            <event name='lady'>Talk to the lady</event>
                            <event name='thievery'>Help grandma</event>
                        </events>
                    </scene>
                </actions>"
        },
        {"ptBR", @"<actions>
                    <scene id='Menu'>
                        <events>
                            <event name='start'>INICIAR</event>
                            <event name='credits'>CREDITOS</event>
                            <event name='back'>VOLTAR</event>
                        </events>    
                    </scene>
                    <scene id='Cen01_Quarto'>
                        <events>
                            <event name='cat'>Carinho no gato</event>
                            <event name='door'>Sair do quarto</event>
                        </events>
                    </scene>
                    <scene id='Cen02_Cozinha'>
                        <events>
                            <event name='door'>Ir para a sala</event>
                            <event name='geladeira'>Abrir geladeira</event>
                        </events>
                    </scene>
                    <scene id='Cen03_Sala'>
                        <events>
                            <event name='dad'>Conversar com pai</event>
                            <event name='dad_remote'>Entregar controle</event>
                            <event name='dad_decision'>PEGAR                    IGNORAR</event>
                            <event name='remote'>Pegar o controle</event>
                            <event name='door'>Ir trabalhar</event>
                        </events>
                    </scene>
                    <scene id='Cen05_Parque'>
                        <events>
                            <event name='door'>Voltar para casa</event>
                            <event name='churrosSeller'>Pedir um churros</event>
                            <event name='lady'>Conversar com a moça</event>
                            <event name='thievery'>Ajudar vovózinha</event>
                        </events>
                    </scene>
                </actions>"
        }
    };

    private static Dictionary<string, XmlNodeList> scenesAction = new Dictionary<string,XmlNodeList>();

    private static StringReader getActions(string language)
    {
        if (actions.ContainsKey(language))
            return new StringReader(actions[language]);
        else
        {
            Debug.LogError("ActionData: Language could not be loaded");
            return null;
        }
    }

    private static XmlNodeList getScenesActions(string language = "US")
    {
        XmlDocument xmlDoc = new XmlDocument();

        xmlDoc.Load(ActionsData.getActions(language));

        return xmlDoc.GetElementsByTagName("scene");
    }

    private static void loadScenesDictionary(string language = "US")
    {
        XmlNodeList scenes = getScenesActions(language);
        
        foreach(XmlNode s in scenes)
        {
            string sceneId = s.Attributes.Item(0).InnerText;
            XmlNodeList sceneEvents = s.FirstChild.ChildNodes;
            scenesAction.Add(sceneId, sceneEvents);
            //Debug.Log("SceneId: " + sceneId + " events loaded");
        }
        
        Debug.Log("ActionData: " + scenesAction.Keys.Count + " Scenes loaded");
    }

    public static Dictionary<string, string> getSceneEventsActionText(string sceneId)
    {
        if (scenesAction.ContainsKey(sceneId))
        {
            Dictionary<string, string> events = new Dictionary<string, string>();

            XmlNodeList eventsList = scenesAction[sceneId];

            foreach (XmlNode e in eventsList)
            {
                string eventName = e.Attributes.Item(0).InnerText;
                string eventText = e.InnerText;
                events.Add(eventName, eventText);
            }

            return events; 
        }
        Debug.LogError("SceneId key: "+ sceneId +" not found!");
        return null;
    }

    public static bool changeLanguage(string language)
    {
        if(actions.ContainsKey(language))
        {   
            scenesAction.Clear();
            loadScenesDictionary(language);
            return true;
        }
        Debug.LogError("Language " + language + " not found!");
        return false;
    }

    void Awake()
    {
        DontDestroyOnLoad(this);
        loadScenesDictionary();
    }

}