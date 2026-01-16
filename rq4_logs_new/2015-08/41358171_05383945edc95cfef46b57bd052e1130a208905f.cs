using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.IO;
public class Dealer : MonoBehaviour {

    public TextAsset GameAsset;

    static string Cube_Character = "";
    static string Cylinder_Character = "";
    static string Capsule_Character = "";
    static string Sphere_Character = "";

    List<Dictionary<string, string>> cards = new List<Dictionary<string, string>>();
    Dictionary<string, string> obj;

    void Start()
    { //Timeline of the Level creator
        GetCards();
    }

    public void GetCards()
    {
        XmlDocument CardDB = new XmlDocument(); // xmlDoc is the new xml document.
        CardDB.LoadXml(GameAsset.text); // load the file.
        XmlNodeList cardList = CardDB.GetElementsByTagName("card"); // array of the level nodes.

        foreach (XmlNode cardInfo in cardList)
        {
            XmlNodeList cardContent = cardInfo.ChildNodes;
            obj = new Dictionary<string, string>(); // Create a object(Dictionary) to colect the both nodes inside the level node and then put into levels[] array.

            foreach (XmlNode cardItens in cardContent) // levels itens nodes.
            {
                if (cardItens.Name == "name")
                {
                    obj.Add("name", cardItens.InnerText); // put this in the dictionary.
                }

                if (cardItens.Name == "icon")
                {
                    obj.Add("icon", cardItens.InnerText); // put this in the dictionary.
                }

                if (cardItens.Name == "level")
                {
                    obj.Add("level", cardItens.InnerText); // put this in the dictionary.
                }

                if (cardItens.Name == "points")
                {
                    obj.Add("points", cardItens.InnerText); // put this in the dictionary.
                }

                if (cardItens.Name == "object")
                {
                    switch (cardItens.Attributes["name"].Value)
                    {
                        case "Cube": obj.Add("Cube", cardItens.InnerText); break; // put this in the dictionary.
                        case "Cylinder": obj.Add("Cylinder", cardItens.InnerText); break; // put this in the dictionary.
                        case "Capsule": obj.Add("Capsule", cardItens.InnerText); break; // put this in the dictionary.
                        case "Sphere": obj.Add("Sphere", cardItens.InnerText); break; // put this in the dictionary.
                    }
                }

                if (cardItens.Name == "finaltext")
                {
                    obj.Add("finaltext", cardItens.InnerText); // put this in the dictionary.
                }
            }
            cards.Add(obj); // add whole obj dictionary in the levels[].
        }
    }
}