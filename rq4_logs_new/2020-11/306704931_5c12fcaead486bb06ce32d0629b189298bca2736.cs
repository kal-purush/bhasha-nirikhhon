using System.Xml;
using UnityEngine;


namespace JevLogin
{
    public sealed class XMLData : IData<SaveData>
    {
        public SaveData Load(string path = null)
        {
            throw new System.NotImplementedException();
        }

        public void Save(SaveData data, string path = null)
        {
            var xmlDoc = new XmlDocument();

            XmlNode rootNode = xmlDoc.CreateElement("Player");
            xmlDoc.AppendChild(rootNode);

            var element = xmlDoc.CreateElement("Name");
            element.SetAttribute("value", data.Name);
            rootNode.AppendChild(element);

            element = xmlDoc.CreateElement("PosX");
            element.SetAttribute("value", data.Position.X.ToString());
            rootNode.AppendChild(element);

            element = xmlDoc.CreateElement("PosY");
            element.SetAttribute("value", data.Position.Y.ToString());
            rootNode.AppendChild(element);

            element = xmlDoc.CreateElement("PosZ");
            element.SetAttribute("value", data.Position.Z.ToString());
            rootNode.AppendChild(element);

            element = xmlDoc.CreateElement("IsEnabled");
            element.SetAttribute("value", data.IsEnabled.ToString());
            rootNode.AppendChild(element);

            XmlNode userNode = xmlDoc.CreateElement("Info");
            var attribute = xmlDoc.CreateAttribute("Unity");
            attribute.Value = Application.unityVersion;
            userNode.Attributes.Append(attribute);
            userNode.InnerText = "System Language: " + Application.systemLanguage;
            rootNode.AppendChild(userNode);

            xmlDoc.Save(path);
        }
    }
}