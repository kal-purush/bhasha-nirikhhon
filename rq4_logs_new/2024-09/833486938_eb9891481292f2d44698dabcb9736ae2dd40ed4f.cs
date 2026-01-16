using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Xml;
using UnityEditor.Experimental.GraphView;

public class CutsceneLoader
{
    public static void loadCutscene(Cutscene nCutscene)
    {
        XmlReaderSettings settings = new XmlReaderSettings();

        TextAsset textAsset = Resources.Load<TextAsset>("CutsceneData/" + nCutscene.resourceString);
        XmlDocument xmlDocument = new XmlDocument();
        xmlDocument.LoadXml(textAsset.text);

        Debug.Log(xmlDocument.SelectNodes("/cutScene/sprite").Count);
        XmlReader reader = XmlReader.Create("Intro.xml", settings);

        string spriteName = "";
        string spritePath = "";

        foreach( XmlNode node in xmlDocument.SelectNodes("/cutScene/sprite")){
            
            foreach(XmlNode nNode in node.ChildNodes)
            {
                if (nNode.Name == "name")
                {
                    spriteName = nNode.InnerText;
                }
                else if (nNode.Name == "path")
                {
                    spritePath = nNode.InnerText;
                }
            }
            nCutscene.addSprite(spriteName, spritePath);
        }

        string frameSpeaker = "";
        string frameSpriteName = "";
        string frameDialogue = "";
        int frameTrigger = 0;
        foreach (XmlNode node in xmlDocument.SelectNodes("/cutScene/frame"))
        {

            foreach (XmlNode nNode in node.ChildNodes)
            {
                if (nNode.Name == "speaker")
                {
                    frameSpriteName = nNode.InnerText;
                    frameSpeaker = frameSpriteName;
                    if(nNode.Attributes.Count > 0)
                    {
                        frameSpeaker = nNode.Attributes[0].Value;
                    }
                }

                else if (nNode.Name == "dialogue")
                {
                    frameTrigger = 0;
                    frameDialogue = nNode.InnerText;
                    if(nNode.Attributes.Count > 0)
                    {
                        frameTrigger = int.Parse(nNode.Attributes[0].Value);
                    }
                }
            }
            nCutscene.addFrame(frameSpeaker, frameDialogue, frameSpriteName, frameTrigger);
        }

            //        foreach(XmlNode node in xmlDocument.SelectNodes())
            /*
                    while (reader)
                    {
                        Debug.Log(reader.Name);
                        Debug.Log(reader.Value);

                        switch (reader.Name)
                        {
                            case "sprite":
                                loadSprite(reader, nCutscene);
                            break;

                            case "frame":
                                loadFrame(reader, nCutscene);
                            break;

                        }
                    }
            */
        }

        static void loadSprite(XmlReader reader,Cutscene nCutscene)
    {
        string spritePath = "";
        string spriteName = "";
        for (int x = 0; x <= 1; x++)
        {
            reader.Read();
            switch (reader.Name)
            {
                case "name":
                    spriteName = reader.Value;
                break;

                case "path":
                    spritePath = reader.Value;
                break;
            }
        }
        nCutscene.addSprite(spriteName, spritePath);
    }

        static void loadFrame(XmlReader reader, Cutscene nCutscene)
    {
        string speakerName = "";
        string spriteName = "";
        string dialogue = "";
        int frameTrigger = 0;
        for(int x = 0; x <= 2; x++)
        {
            reader.Read();
            switch (reader.Name)
            {
                case "sprite":
                    spriteName = reader.Value;
                    speakerName = spriteName;
                    if(reader.AttributeCount > 0)
                    {
                        speakerName = reader.GetAttribute(1);
                    }
                break;

                case "dialogue":
                    dialogue = reader.Value;
                    if(reader.AttributeCount > 0)
                    {
                        frameTrigger = int.Parse(reader.GetAttribute(1));
                    }
                break;
            }
        }
        nCutscene.addFrame(speakerName, dialogue, spriteName, frameTrigger);
    }
}